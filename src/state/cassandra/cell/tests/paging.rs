//! Fetch hints change page size without changing shared prepared statements.

use super::fixture;
use crate::state::cassandra::cell::read::scan_statement;
use crate::state::cell_key::{Direction, EdgeKind};
use color_eyre::Result;
use std::num::NonZeroUsize;

#[tokio::test]
async fn scan_fetch_hint_preserves_prepared_defaults() -> Result<()> {
    let fixture = fixture().await?;
    for reads in [&fixture.queries.values, &fixture.queries.presence] {
        for direction in [Direction::Forward, Direction::Backward] {
            for edge in [EdgeKind::Included, EdgeKind::Excluded, EdgeKind::Unbounded] {
                let prepared = reads.scan.select(direction, edge);
                let default = prepared.get_page_size();
                assert_eq!(scan_statement(prepared, None).get_page_size(), default);
                for (hint, expected) in [(1, 9_i32), (128, 136_i32), (usize::MAX, default)] {
                    let statement = scan_statement(prepared, NonZeroUsize::new(hint));
                    assert_eq!(statement.get_page_size(), expected.min(default));
                    assert_eq!(prepared.get_page_size(), default);
                }
            }
        }
    }
    Ok(())
}
