use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt, HashSet};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use tokio::runtime::Builder;

use crate::consumer::partition::offsets::{Action, OffsetTracker, Operation};
use crate::Offset;

#[derive(Clone, Debug)]
struct Actions(Vec<Action>);

#[quickcheck]
fn tracks_commit_watermark(actions: Actions) -> TestResult {
    Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(tracks_commit_watermark_impl(actions))
}

async fn tracks_commit_watermark_impl(Actions(actions): Actions) -> TestResult {
    let version = Arc::default();
    let tracker = OffsetTracker::new(actions.len() + 1, version);
    let mut test_offsets = BTreeMap::default();
    let mut commits = HashMap::with_capacity(actions.len());

    for action in &actions {
        match action.operation {
            Operation::Take => {
                let commit = tracker.take(action.offset).await.unwrap();
                commits.insert(action.offset, commit);
                test_offsets.insert(action.offset, action.operation);
            }
            Operation::Commit => {
                if let Some(commit) = commits.remove(&action.offset) {
                    commit.commit();
                    test_offsets.insert(action.offset, action.operation);
                }
            }
        }
    }

    drop(commits);

    let expected = test_offsets
        .iter()
        .take_while(|(_, &action)| action == Operation::Commit)
        .last()
        .map(|(&offset, _)| offset);

    let actual = tracker.shutdown().await;

    if expected == actual {
        TestResult::passed()
    } else {
        TestResult::error(format!("{expected:?} != {actual:?} ({test_offsets:?})"))
    }
}

impl Arbitrary for Actions {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut offset: Offset = 0;
        let mut takes = HashSet::default();
        let mut actions = Vec::<Action>::arbitrary(g);
        let gaps: Vec<Offset> = (0..4).collect();

        for action in actions.iter_mut() {
            if takes.is_empty() || bool::arbitrary(g) {
                let gap = *g.choose(&gaps).unwrap();
                offset += gap as Offset;
                action.offset = offset;
                action.operation = Operation::Take;
                takes.insert(offset);
                offset += 1;
            } else {
                action.offset = *g
                    .choose(&takes.iter().copied().collect::<Vec<_>>())
                    .unwrap();

                action.operation = Operation::Commit;
                takes.remove(&action.offset);
            }
        }

        Self(actions)
    }
}

impl Arbitrary for Action {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            offset: u16::arbitrary(g) as Offset,
            operation: Operation::arbitrary(g),
        }
    }
}

impl Arbitrary for Operation {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Operation::Take
        } else {
            Operation::Commit
        }
    }
}
