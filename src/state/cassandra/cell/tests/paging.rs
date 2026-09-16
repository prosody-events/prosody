//! Fetch hints change page size without changing shared prepared statements.

use super::fixture;
use crate::state::cassandra::cell::read::scan_statement;
use crate::state::cell_key::{Direction, EdgeKind};
use crate::state::store::{CELL_BATCH, FetchSchedule};
use color_eyre::Result;
use std::num::NonZeroUsize;

#[tokio::test]
async fn scan_fetch_hint_preserves_prepared_defaults() -> Result<()> {
    let fixture = fixture().await?;
    for reads in [&fixture.queries.values, &fixture.queries.presence] {
        for direction in [Direction::Forward, Direction::Backward] {
            for edge in [EdgeKind::Included, EdgeKind::Excluded, EdgeKind::Unbounded] {
                let prepared = reads.scan.select(direction, edge);
                let default = NonZeroUsize::new(usize::try_from(prepared.get_page_size())?)
                    .unwrap_or(NonZeroUsize::MIN);
                for size in [NonZeroUsize::MIN, CELL_BATCH, default] {
                    let statement = scan_statement(prepared, size);
                    assert_eq!(statement.get_page_size(), i32::try_from(size.get())?);
                    assert_eq!(prepared.get_page_size(), i32::try_from(default.get())?);
                }
                let mut fetch = FetchSchedule::new(NonZeroUsize::new(1), default);
                let mut expected = NonZeroUsize::MIN;
                loop {
                    assert_eq!(fetch.next(), expected);
                    if expected == default {
                        break;
                    }
                    expected = expected
                        .saturating_mul(NonZeroUsize::MIN.saturating_add(1))
                        .min(default);
                }
            }
        }
    }
    Ok(())
}
