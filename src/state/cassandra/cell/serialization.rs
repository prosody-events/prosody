use super::{
    BatchRow, CellBatchRow, CellKind, GapBetweenRow, GapEdgeRow, GapSectionRow, INITIAL_VERSION,
    KeyRow, MarkerWriteRow, PreparedStatement, ResolvedRow, RowSerializationContext, RowShape,
    RowWriter, SerializationError, SerializeRow, StageRow,
};

impl BatchRow for CellBatchRow<'_> {
    fn statement(&self) -> &PreparedStatement {
        self.statement
    }
}

impl SerializeRow for CellBatchRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        match &self.row {
            RowShape::Stage(row) => row.serialize(ctx, writer),
            RowShape::Resolved(row) => row.serialize(ctx, writer),
            RowShape::MarkerWrite(row) => row.serialize(ctx, writer),
            RowShape::Key(row) => row.serialize(ctx, writer),
            RowShape::GapSection(row) => row.serialize(ctx, writer),
            RowShape::GapEdge(row) => row.serialize(ctx, writer),
            RowShape::GapBetween(row) => row.serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for StageRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        // The `ttl` arms differ only by the leading `USING TTL ?` column the
        // no-TTL statement omits; `kind` leads the clustering key.
        match self.ttl {
            Some(ttl) => (
                ttl,
                self.data,
                self.prev_data,
                self.encoding,
                self.version,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
            None => (
                self.data,
                self.prev_data,
                self.encoding,
                self.version,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for ResolvedRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        match self.ttl {
            Some(ttl) => (
                ttl,
                self.data,
                self.encoding,
                self.version,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
            None => (
                self.data,
                self.encoding,
                self.version,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for MarkerWriteRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        // The `ttl` arms differ only by the leading `USING TTL ?` column the
        // no-TTL statement omits (as `StageRow`). The payload always carries
        // this build's encoding/version stamps.
        match self.ttl {
            Some(ttl) => (
                ttl,
                self.payload,
                self.encoding,
                INITIAL_VERSION,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Marker,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
            None => (
                self.payload,
                self.encoding,
                INITIAL_VERSION,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Marker,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for KeyRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        (
            a.pk.segment_id,
            a.pk.key,
            a.pk.state_type,
            a.pk.name,
            self.kind,
            a.section,
            a.coordinate,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for GapSectionRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        (
            self.pk.segment_id,
            self.pk.key,
            self.pk.state_type,
            self.pk.name,
            CellKind::Cell,
            self.section,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for GapEdgeRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        (
            self.pk.segment_id,
            self.pk.key,
            self.pk.state_type,
            self.pk.name,
            CellKind::Cell,
            self.section,
            self.coordinate,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for GapBetweenRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        (
            self.pk.segment_id,
            self.pk.key,
            self.pk.state_type,
            self.pk.name,
            CellKind::Cell,
            self.section,
            self.low,
            self.high,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}
