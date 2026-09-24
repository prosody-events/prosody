//! Committed reads through the cell cache.

use super::metrics::{CacheResult, Source};
use super::{Cached, expiry_at, warn_skip};
use crate::state::cell::Projection;
use crate::state::cell_key::{CellKey, CellRef, Scan, Section};
use crate::state::fjall::CacheRead;
use crate::state::identity::CollectionId;
use crate::state::store::{CacheBatch, CellRead, Durable, ReadBatch};
use futures::Stream;
use quanta::Instant;

impl<L: CellRead<P>, P: Projection> CellRead<P> for Cached<L> {
    async fn read<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: CellRef<'a>,
    ) -> Result<Durable<P>, Self::Error> {
        let started = Instant::now();
        if self.fjall.is_disabled() {
            let loaded = CellRead::<P>::read(&self.lower, collection, cell).await;
            self.metrics.point(
                P::NAME,
                started,
                Source::Store,
                CacheResult::Disabled,
                &loaded,
            );
            return loaded;
        }
        let cache_result = match self.fjall.get::<P>(collection, cell).await {
            Ok(CacheRead::Hit(hit)) => {
                let loaded = Ok(hit);
                self.metrics
                    .point(P::NAME, started, Source::Cache, CacheResult::Hit, &loaded);
                return loaded;
            }
            Ok(CacheRead::Miss) => CacheResult::Miss,
            Ok(CacheRead::Expired) => CacheResult::Expired,
            Ok(CacheRead::Corrupt) => {
                self.metrics.cache_error("get", "lookup");
                CacheResult::Error
            }
            Err(error) => {
                warn_skip("read", &error);
                self.metrics.cache_error("get", "lookup");
                let loaded = CellRead::<P>::read(&self.lower, collection, cell).await;
                self.metrics
                    .point(P::NAME, started, Source::Store, CacheResult::Error, &loaded);
                return loaded;
            }
        };
        let stamped_at = self.fjall.clock().now_ms();
        let loaded = async {
            let (committed, remaining) = CellRead::<P>::read(&self.lower, collection, cell).await?;
            if let Err(error) = self
                .fjall
                .put::<P>(
                    collection,
                    cell,
                    committed.clone(),
                    expiry_at(stamped_at, remaining),
                )
                .await
            {
                warn_skip("populate", &error);
                self.metrics.cache_error("get", "fill");
            }
            Ok((committed, remaining))
        }
        .await;
        self.metrics
            .point(P::NAME, started, Source::Store, cache_result, &loaded);
        loaded
    }

    /// Reads the whole lower batch after any miss and publishes only probe
    /// misses. Partial refetch requires a benchmark before it can replace
    /// this rule.
    async fn read_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a ReadBatch<'_>,
    ) -> Result<CacheBatch<P>, Self::Error> {
        let started = Instant::now();
        if self.fjall.is_disabled() {
            let loaded = CellRead::<P>::read_many(&self.lower, collection, section, batch).await;
            self.metrics.batch(
                batch.len(),
                P::NAME,
                started,
                Source::Store,
                CacheResult::Disabled,
                &loaded,
            );
            return loaded;
        }
        let probes = match self.fjall.get_batch::<P>(collection, section, batch).await {
            Ok(probes) => {
                let hits = probes.try_map(|probe| match probe {
                    CacheRead::Hit(hit) => Ok(hit.clone()),
                    CacheRead::Miss | CacheRead::Expired | CacheRead::Corrupt => Err(()),
                });
                if let Ok(hits) = hits {
                    let loaded = Ok(hits);
                    self.metrics.batch(
                        batch.len(),
                        P::NAME,
                        started,
                        Source::Cache,
                        CacheResult::Hit,
                        &loaded,
                    );
                    return loaded;
                }
                probes
            }
            Err(error) => {
                warn_skip("read batch", &error);
                self.metrics.cache_error("get_many", "lookup");
                let loaded =
                    CellRead::<P>::read_many(&self.lower, collection, section, batch).await;
                self.metrics.batch(
                    batch.len(),
                    P::NAME,
                    started,
                    Source::Store,
                    CacheResult::Error,
                    &loaded,
                );
                return loaded;
            }
        };
        let stamped_at = self.fjall.clock().now_ms();
        let loaded = async {
            let filled = CellRead::<P>::read_many(&self.lower, collection, section, batch).await?;
            let projected = batch
                .iter()
                .zip(filled.iter())
                .zip(probes.iter())
                .filter(|(_, probe)| !matches!(probe, CacheRead::Hit(_)))
                .map(|((coordinate, (committed, remaining)), _)| {
                    (
                        CellRef {
                            section,
                            coordinate,
                        },
                        committed.clone(),
                        expiry_at(stamped_at, *remaining),
                    )
                });
            if let Err(error) = self.fjall.put_batch::<P>(collection, projected).await {
                warn_skip("populate batch", &error);
                self.metrics.cache_error("get_many", "fill");
            }
            Ok(filled)
        }
        .await;
        self.metrics.batch(
            batch.len(),
            P::NAME,
            started,
            Source::Store,
            CacheResult::NotAllHit,
            &loaded,
        );
        loaded
    }

    fn scan<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), Self::Error>> + Send + use<'a, L, P> {
        CellRead::<P>::scan(&self.lower, collection, scan)
    }
}
