use crate::Key;
use crate::propagator::new_propagator;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::cassandra::queries::Queries;
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use async_stream::try_stream;
use derive_builder::Builder;
use educe::Educe;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use scylla::_macro_internal::{
    CellWriter, ColumnType, DeserializationError, DeserializeValue, FrameSlice, SerializationError,
    SerializeValue, TypeCheckError, WrittenCellProof,
};
use scylla::client::session::Session;
use scylla::cluster::metadata::NativeType;
use scylla::errors::{
    ExecutionError, IntoRowsResultError, MaybeFirstRowError, NewSessionError, NextRowError,
    PagerExecutionError, PrepareError, RowsError, UseKeyspaceError,
};
use std::collections::HashMap;
use std::fmt::Formatter;
use std::ops::RangeInclusive;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use validator::Validate;

mod embedded_migrator;
mod queries;

#[derive(Builder, Clone, Educe, Validate)]
#[educe(Debug)]
pub struct CassandraConfiguration {
    datacenter: Option<String>,
    rack: Option<String>,
    nodes: Vec<String>,
    keyspace: String,
    user: Option<String>,

    #[educe(Debug(ignore))]
    password: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CassandraTriggerStore(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    queries: Queries,
    propagator: TextMapCompositePropagator,
}

impl CassandraTriggerStore {
    pub async fn new(config: CassandraConfiguration) -> Result<Self, CassandraTriggerStoreError> {
        Ok(Self(Arc::new(Inner {
            queries: Box::pin(Queries::new(config)).await?,
            propagator: new_propagator(),
        })))
    }

    fn session(&self) -> &Session {
        &self.0.queries.session
    }

    fn queries(&self) -> &Queries {
        &self.0.queries
    }

    fn propagator(&self) -> &TextMapCompositePropagator {
        &self.0.propagator
    }
}

impl TriggerStore for CassandraTriggerStore {
    type Error = CassandraTriggerStoreError;

    #[instrument(skip(self), err)]
    async fn insert_segment(&self, segment: Segment) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().insert_segment,
                (segment.id, segment.name, segment.slab_size),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error> {
        let row = self
            .session()
            .execute_unpaged(&self.queries().get_segment, (segment_id,))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(String, CompactDuration)>()?;

        let Some((name, slab_size)) = row else {
            return Ok(None);
        };

        Ok(Some(Segment {
            id: *segment_id,
            name,
            slab_size,
        }))
    }

    #[instrument(skip(self), err)]
    async fn delete_segment(&self, segment_id: &SegmentId) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(&self.queries().delete_segment, (segment_id,))
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    fn get_slabs(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_slabs.clone(), (segment_id,))
                .await?
                .rows_stream::<(Option<i32>,)>()?;

            pin_mut!(stream);
            while let Some((value,)) = stream.try_next().await? {
                let Some(value) = value else {
                    continue;
                };

                yield SlabId::from_le_bytes(value.to_le_bytes())
            }
        }
    }

    #[instrument(skip(self))]
    fn get_slab_range(
        &self,
        segment_id: &SegmentId,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        let start = i32::from_le_bytes(range.start().to_le_bytes());
        let end = i32::from_le_bytes(range.end().to_le_bytes());

        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_slab_range.clone(), (segment_id, start, end))
                .await?
                .rows_stream::<(Option<i32>,)>()?;

            pin_mut!(stream);
            while let Some((value,)) = stream.try_next().await? {
                let Some(value) = value else {
                    continue;
                };

                yield SlabId::from_le_bytes(value.to_le_bytes())
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn insert_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().insert_slab,
                (segment_id, i32::from_le_bytes(slab_id.to_le_bytes())),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_slab,
                (segment_id, i32::from_le_bytes(slab_id.to_le_bytes())),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    fn get_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let segment_id = slab.segment_id();
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());

        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_slab_triggers.clone(), (segment_id, slab_id))
                .await?
                .rows_stream::<(String, CompactDateTime, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, span_map)) = stream.try_next().await? {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger");
                span.set_parent(context);

                yield Trigger {
                    key: key.into(),
                    time,
                    span,
                }
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator()
            .inject_context(&trigger.span.context(), &mut span_map);

        self.session()
            .execute_unpaged(
                &self.queries().insert_slab_trigger,
                (
                    slab.segment_id(),
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                    trigger.key.as_str(),
                    trigger.time,
                    span_map,
                ),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn delete_slab_trigger(
        &self,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_slab_trigger,
                (
                    slab.segment_id(),
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                    key.as_str(),
                    time,
                ),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().clear_slab_triggers,
                (
                    slab.segment_id(),
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                ),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_key_times.clone(), (segment_id, key.as_str()))
                .await?
                .rows_stream::<(CompactDateTime,)>()?;

            pin_mut!(stream);
            while let Some((time,)) = stream.try_next().await? {
                yield time
            }
        }
    }

    #[instrument(skip(self))]
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_key_triggers.clone(), (segment_id, key.as_str()))
                .await?
                .rows_stream::<(String, CompactDateTime, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, span_map)) = stream.try_next().await? {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_key_trigger");
                span.set_parent(context);

                yield Trigger {
                    key: key.into(),
                    time,
                    span,
                }
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator()
            .inject_context(&trigger.span.context(), &mut span_map);

        self.session()
            .execute_unpaged(
                &self.queries().insert_key_trigger,
                (segment_id, trigger.key.as_str(), trigger.time, span_map),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_key_trigger,
                (segment_id, key.as_str(), time),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().clear_key_triggers,
                (segment_id, key.as_str()),
            )
            .await?;

        Ok(())
    }
}

impl SerializeValue for CompactDuration {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i32::from(*self).serialize(typ, writer)
    }
}

impl SerializeValue for CompactDateTime {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i32::from(*self).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CompactDuration {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(InnerError::IntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(CompactDuration::from(i32::deserialize(typ, v)?))
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CompactDateTime {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(InnerError::IntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(CompactDateTime::from(i32::deserialize(typ, v)?))
    }
}

#[derive(Debug, Error)]
pub enum InnerError {
    #[error(transparent)]
    Session(#[from] NewSessionError),

    #[error("migration failed: {0:#}")]
    Migration(String),

    #[error("failed to set keyspace: {0:#}")]
    UseKeyspace(#[from] UseKeyspaceError),

    #[error("failed to prepare statement: {0:#}")]
    Prepare(#[from] PrepareError),

    #[error("failed to execute statement: {0:#}")]
    Execution(#[from] ExecutionError),

    #[error("invalid type: {0:#}")]
    TypeCheck(#[from] TypeCheckError),

    #[error("failed to retrieve the next row: {0:#}")]
    NextRow(#[from] NextRowError),

    #[error("failed to retrieve the next page: {0:#}")]
    PageExecution(#[from] PagerExecutionError),

    #[error("failed to retrieve the first row: {0:#}")]
    MaybeFirstRow(#[from] MaybeFirstRowError),

    #[error("failed to get rows: {0:#}")]
    IntoRows(#[from] IntoRowsResultError),

    #[error("rows error: {0:#}")]
    Rows(#[from] RowsError),

    #[error("expected and integer type")]
    IntExpected,
}

pub struct CassandraTriggerStoreError(Box<InnerError>);

impl<E> From<E> for CassandraTriggerStoreError
where
    InnerError: From<E>,
{
    fn from(err: E) -> Self {
        Self(Box::new(InnerError::from(err)))
    }
}

impl std::fmt::Debug for CassandraTriggerStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for CassandraTriggerStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for CassandraTriggerStoreError {}

#[cfg(test)]
mod test {
    use super::{CassandraConfiguration, CassandraTriggerStore};
    use crate::trigger_store_tests;

    // Run the full suite of TriggerStore compliance tests on this implementation.
    trigger_store_tests!(
        CassandraTriggerStore,
        CassandraTriggerStore::new(CassandraConfiguration {
            datacenter: None,
            rack: None,
            nodes: vec!["localhost:9042".to_owned()],
            keyspace: "prosody".to_owned(),
            user: None,
            password: None,
        }),
        25
    );
}
