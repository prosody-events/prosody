//! Queries acquire their session only when the stream is polled.

use super::support::{GROUP_A, ScriptedEnv, source_state_key, topic};
use crate::Key;
use crate::codec::JsonCodec;
use crate::state::descriptor::{deque_state, map_state, set_state};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state_reader::StateReaderError;
use color_eyre::Result;
use futures::StreamExt;

/// Builds one stream from a borrowed reader and checks that it reads nothing
/// before its first poll.
macro_rules! check_lazy {
    ($descriptor:expr, |$reader:ident, $key:ident| $stream:expr) => {{
        let env = ScriptedEnv::new($descriptor)?;
        let key = Key::from("user-1");
        let tp = topic("lazy-source");
        let segment = source_state_key(tp, GROUP_A, &key, env.count)?.segment_id;
        let reader = env.reader_eager()?;
        let $reader = &reader;

        drop({
            let $key = key.clone();
            $stream
        });
        let exhausted = {
            let invalid = {
                let $key = Key::from("");
                $stream
            };
            assert_eq!(env.publications.reads(), 0);
            assert_eq!(env.identities.reads(), 0);
            assert_eq!(env.cells.reads(segment), 0);

            futures::pin_mut!(invalid);
            assert!(matches!(
                invalid.next().await,
                Some(Err(StateReaderError::EmptyKey))
            ));
            invalid.next().await.is_none()
        };
        assert!(exhausted);

        let valid = {
            let $key = key;
            $stream
        };
        assert_eq!(env.publications.reads(), 0);

        // A publication created after the stream must be visible on its first
        // poll.
        env.publish(GROUP_A, tp).await;
        futures::pin_mut!(valid);
        assert!(valid.next().await.is_none());
        assert_eq!(env.publications.reads(), 1);
        assert_eq!(env.identities.reads(), 1);
        assert!(env.cells.reads(segment) > 0);
    }};
}

#[tokio::test]
async fn queries_acquire_on_first_poll() -> Result<()> {
    check_lazy!(
        map_state::<Utf8KeyCodec, JsonCodec>("lazy-map"),
        |reader, key| reader.entries(key).stream()
    );
    check_lazy!(
        map_state::<Utf8KeyCodec, JsonCodec>("lazy-keys"),
        |reader, key| reader.keys(key).stream()
    );
    check_lazy!(set_state::<Utf8KeyCodec>("lazy-set"), |reader, key| reader
        .keys(key)
        .stream());
    check_lazy!(deque_state::<JsonCodec>("lazy-deque"), |reader, key| reader
        .values(key)
        .stream());
    Ok(())
}

/// Builds a query that selects nothing and checks that it ends without a
/// session, even over a published source.
macro_rules! check_empty {
    ($descriptor:expr, |$reader:ident, $key:ident| $stream:expr) => {{
        let env = ScriptedEnv::new($descriptor)?;
        let key = Key::from("user-1");
        let tp = topic("empty-source");
        let segment = source_state_key(tp, GROUP_A, &key, env.count)?.segment_id;
        env.publish(GROUP_A, tp).await;
        let reader = env.reader_eager()?;
        let $reader = &reader;
        let empty = {
            let $key = key;
            $stream
        };
        futures::pin_mut!(empty);
        assert!(empty.next().await.is_none());
        assert_eq!(env.publications.reads(), 0);
        assert_eq!(env.identities.reads(), 0);
        assert_eq!(env.cells.reads(segment), 0);
    }};
}

#[tokio::test]
async fn empty_queries_acquire_nothing() -> Result<()> {
    check_empty!(
        map_state::<Utf8KeyCodec, JsonCodec>("empty-map"),
        |reader, key| reader.entries(key).prefix("ab").prefix("b").stream()
    );
    check_empty!(set_state::<Utf8KeyCodec>("empty-set"), |reader, key| reader
        .keys(key)
        .from("b")
        .to("a")
        .stream());
    check_empty!(deque_state::<JsonCodec>("empty-deque"), |reader, key| {
        reader.values(key).from(5).to(3).stream()
    });
    Ok(())
}
