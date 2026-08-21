use super::*;

/// The erased set preserves membership and key order.
/// FALSIFICATION: make erased `insert` call typed `remove`.
#[test]
fn erased_set_parity() -> Result<()> {
    executor::block_on(async {
        let context = parity_context::<Value>()?;
        let handle: BoxSetState = context
            .set_state(SET_NAME)
            .map_err(|error| eyre!("vend set: {error}"))?;
        handle
            .insert("b".to_owned())
            .await
            .map_err(|error| eyre!("insert: {error}"))?;
        handle
            .insert("a".to_owned())
            .await
            .map_err(|error| eyre!("insert: {error}"))?;
        assert!(
            handle
                .contains("a".to_owned())
                .await
                .map_err(|error| eyre!("contains: {error}"))?
        );
        assert_eq!(
            handle
                .contains_many(vec!["a".to_owned(), "c".to_owned()])
                .await
                .map_err(|error| eyre!("contains many: {error}"))?,
            vec![true, false]
        );
        assert_eq!(
            drain_cursor(&handle.keys(KeyScanConfig::default())).await?,
            vec!["a", "b"]
        );
        handle
            .remove("a".to_owned())
            .await
            .map_err(|error| eyre!("remove: {error}"))?;
        assert!(
            !handle
                .is_empty()
                .await
                .map_err(|error| eyre!("empty: {error}"))?
        );
        handle
            .clear()
            .await
            .map_err(|error| eyre!("clear: {error}"))?;
        assert!(
            handle
                .is_empty()
                .await
                .map_err(|error| eyre!("empty: {error}"))?
        );
        Ok(())
    })
}
