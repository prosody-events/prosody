use super::*;

/// Reads the provisional cells named by the current Staged row without
/// mutation. Physical marker completeness is checked separately by the shape
/// probes.
pub(crate) trait StageInspection: CellStore {
    fn staged_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        async_stream::try_stream! {
            if let Some(marker) = self.marker_state(collection).await?.staged {
                for key in marker.staged() {
                    if let Some(cell) = self.provisional_cell_at(collection, key).await? {
                        yield (key.clone(), cell);
                    }
                }
            }
        }
    }
}
impl<S: CellStore> StageInspection for S {}
