//! Queries acquire their session only when the stream is polled.

use super::support::{GROUP_A, ScriptedEnv, source_state_key, topic};
use crate::Key;
use crate::codec::JsonCodec;
use crate::state::descriptor::{StateDescriptor, deque_state, map_state, set_state};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state_reader::backend::ScriptedReaderBackend;
use crate::state_reader::{StateReader, StateReaderError};
use color_eyre::Result;
use futures::{Stream, StreamExt};

#[tokio::test]
async fn queries_acquire_on_first_poll() -> Result<()> {
    Box::pin(check_lazy(
        map_state::<Utf8KeyCodec, JsonCodec>("lazy-map"),
        |reader, key| reader.entries(key).stream(),
    ))
    .await?;
    Box::pin(check_lazy(
        map_state::<Utf8KeyCodec, JsonCodec>("lazy-keys"),
        |reader, key| reader.keys(key).stream(),
    ))
    .await?;
    Box::pin(check_lazy(
        set_state::<Utf8KeyCodec>("lazy-set"),
        |reader, key| reader.keys(key).stream(),
    ))
    .await?;
    Box::pin(check_lazy(
        deque_state::<JsonCodec>("lazy-deque"),
        |reader, key| reader.values(key).stream(),
    ))
    .await
}

async fn check_lazy<D, T, S>(
    descriptor: D,
    stream: impl Fn(StateReader<D, JsonCodec, ScriptedReaderBackend>, Key) -> S,
) -> Result<()>
where
    D: StateDescriptor,
    S: Stream<Item = Result<T, StateReaderError>>,
{
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let tp = topic("lazy-source");
    let segment = source_state_key(tp, GROUP_A, &key, env.count)?.segment_id;

    // Each closure drops its reader before it returns the owned stream.
    drop(stream(env.reader_eager()?, key.clone()));
    let exhausted = {
        let invalid = stream(env.reader_eager()?, Key::from(""));
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

    let valid = stream(env.reader_eager()?, key);
    assert_eq!(env.publications.reads(), 0);

    // A publication created after the stream must be visible on its first poll.
    env.publish(GROUP_A, tp).await;
    futures::pin_mut!(valid);
    assert!(valid.next().await.is_none());
    assert_eq!(env.publications.reads(), 1);
    assert_eq!(env.identities.reads(), 1);
    assert!(env.cells.reads(segment) > 0);
    Ok(())
}
