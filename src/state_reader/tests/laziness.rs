//! Queries acquire their session only when the stream is polled.

use super::support::{GROUP_A, ScriptedEnv, source_state_key, topic};
use crate::Key;
use crate::codec::JsonCodec;
use crate::state::descriptor::{deque_state, map_state, set_state};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state_reader::StateReaderError;
use color_eyre::Result;
use futures::StreamExt;

/// Checks the laziness of one query shape over a borrowed reader.
///
/// `$stream` must be able to select a value and `$empty` must select nothing.
/// Neither stream reads before its first poll. An empty key fails both. A
/// polled empty query reads nothing. The valid query's reads that follow in
/// the same environment prove that the counters observe reads.
macro_rules! check_lazy {
    ($descriptor:expr, |$reader:ident, $key:ident| $stream:expr, $empty:expr) => {{
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
        // An empty key fails both queries before any read.
        {
            let invalid = {
                let $key = Key::from("");
                $stream
            };
            futures::pin_mut!(invalid);
            assert!(matches!(
                invalid.next().await,
                Some(Err(StateReaderError::EmptyKey))
            ));
            assert!(invalid.next().await.is_none());
        }
        {
            let invalid = {
                let $key = Key::from("");
                $empty
            };
            futures::pin_mut!(invalid);
            assert!(matches!(
                invalid.next().await,
                Some(Err(StateReaderError::EmptyKey))
            ));
            assert!(invalid.next().await.is_none());
        }
        assert_eq!(env.publications.reads(), 0);

        let valid = {
            let $key = key.clone();
            $stream
        };
        assert_eq!(env.publications.reads(), 0);

        // A publication created after the stream must be visible on its first
        // poll. An empty query over the same publication still reads nothing.
        env.publish(GROUP_A, tp).await;
        let empty = {
            let $key = key;
            $empty
        };
        futures::pin_mut!(empty);
        assert!(empty.next().await.is_none());
        assert_eq!(env.publications.reads(), 0);
        assert_eq!(env.identities.reads(), 0);
        assert_eq!(env.cells.reads(segment), 0);

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
        |reader, key| reader.entries(key).stream(),
        reader.entries(key).prefix("ab").prefix("b").stream()
    );
    check_lazy!(
        map_state::<Utf8KeyCodec, JsonCodec>("lazy-keys"),
        |reader, key| reader.keys(key).stream(),
        reader.keys(key).from("b").to("a").stream()
    );
    check_lazy!(
        set_state::<Utf8KeyCodec>("lazy-set"),
        |reader, key| reader.keys(key).stream(),
        reader.keys(key).prefix("ab").prefix("b").stream()
    );
    check_lazy!(
        deque_state::<JsonCodec>("lazy-deque"),
        |reader, key| reader.values(key).stream(),
        reader.values(key).from(5).to(3).stream()
    );
    Ok(())
}
