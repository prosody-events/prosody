//! Flat pending response map lifecycle invariants.

use super::{
    MAX_TIMEOUT, POOL, TestCodec, TestCodecError, distinct_indices, failure, names, poll_once,
    register, registry, success,
};
use crate::error::ErrorCategory;
use crate::peer::requester::collect::collect;
use crate::peer::requester::registry::tests::pending_len;
use crate::peer::requester::{RequestError, ResponseError};
use crate::peer::response::ResponseDisposition;
use crate::peer::router::loopback::paused;
use crate::producer::ProducerError;
use color_eyre::Result;
use color_eyre::eyre::bail;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use rdkafka::error::KafkaError;
use std::future::pending;
use std::pin::pin;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
struct ArrivalTrace {
    awaited: Vec<usize>,
    arrivals: Vec<(u8, u32)>,
}

impl Arbitrary for ArrivalTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let count = usize::arbitrary(g) % POOL.len() + 1;
        let awaited = distinct_indices(g, POOL.len(), count);
        let mut arrivals = Vec::arbitrary(g);
        arrivals.truncate(24);
        Self { awaited, arrivals }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let awaited = self.awaited.clone();
        Box::new(self.arrivals.shrink().map(move |arrivals| Self {
            awaited: awaited.clone(),
            arrivals,
        }))
    }
}

/// Each subsystem owns one pending response, whatever order responses arrive
/// in.
#[quickcheck]
fn arrivals_consume_only_their_exact_pending_response(trace: ArrivalTrace) -> TestResult {
    match run_arrivals(trace) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
}

#[tokio::test(start_paused = true)]
async fn each_pending_response_is_removed_once() -> Result<()> {
    let registry = registry();
    let names = names(&["billing", "ledger"])?;
    let registration = register(&registry, &names, MAX_TIMEOUT)?;
    let id = registration.id();

    assert_eq!(
        registry.accept(success(id, &names[1], 2)?),
        ResponseDisposition::Accepted
    );
    assert_eq!(
        registry.accept(success(id, &names[1], 3)?),
        ResponseDisposition::UnknownRequest
    );
    assert_eq!(
        registry.accept(success(id, &names[0], 1)?),
        ResponseDisposition::Accepted
    );

    let results = collect::<TestCodec, _, TestCodecError>(registration, &names, pending()).await?;
    assert_eq!(results.get(&names[0]), Some(&Ok(1)));
    assert_eq!(results.get(&names[1]), Some(&Ok(2)));
    assert_eq!(pending_len(&registry), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn handler_failures_keep_their_message() -> Result<()> {
    let registry = registry();
    let names = names(&["billing"])?;
    let registration = register(&registry, &names, MAX_TIMEOUT)?;
    assert_eq!(
        registry.accept(failure(
            registration.id(),
            &names[0],
            ErrorCategory::Permanent,
            "invalid account",
        )),
        ResponseDisposition::Accepted
    );
    let results = collect::<TestCodec, _, TestCodecError>(registration, &names, async {
        Ok::<(), ProducerError<TestCodecError>>(())
    })
    .await?;
    assert_eq!(
        results.get(&names[0]),
        Some(&Err(ResponseError::Handler {
            message: "invalid account".to_owned(),
        }))
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn responses_do_not_cancel_producer_completion() -> Result<()> {
    let registry = registry();
    let names = names(&["billing"])?;
    let registration = register(&registry, &names, MAX_TIMEOUT)?;
    let id = registration.id();
    let (complete, completed) = oneshot::channel();
    let produce = async {
        completed
            .await
            .map_err(|_| ProducerError::Kafka(KafkaError::Canceled))?;
        Ok::<(), ProducerError<TestCodecError>>(())
    };
    assert_eq!(
        registry.accept(success(id, &names[0], 1)?),
        ResponseDisposition::Accepted
    );

    let mut collected = pin!(collect::<TestCodec, _, TestCodecError>(
        registration,
        &names,
        produce,
    ));
    assert!(
        poll_once(collected.as_mut()).await.is_pending(),
        "responses completed the request before producer completion"
    );
    assert!(complete.send(()).is_ok());
    assert_eq!(collected.await?.get(&names[0]), Some(&Ok(1)));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn every_request_exit_removes_remaining_responses() -> Result<()> {
    let names = names(&["billing", "ledger"])?;

    let registry = registry();
    drop(register(&registry, &names, MAX_TIMEOUT)?);
    assert_eq!(pending_len(&registry), 0);

    let registration = register(&registry, &names, MAX_TIMEOUT)?;
    let results = collect::<TestCodec, _, TestCodecError>(registration, &names, async {
        Ok::<(), ProducerError<TestCodecError>>(())
    })
    .await?;
    assert_eq!(
        results
            .values()
            .filter(|result| **result == Err(ResponseError::Timeout))
            .count(),
        names.len()
    );
    assert_eq!(pending_len(&registry), 0);

    let registration = register(&registry, &names, MAX_TIMEOUT)?;
    let outcome = collect::<TestCodec, _, TestCodecError>(registration, &names, async {
        Err(ProducerError::Kafka(KafkaError::Canceled))
    })
    .await;
    if !matches!(outcome, Err(RequestError::Produce(_))) {
        bail!("a send failure did not fail the request");
    }
    assert_eq!(pending_len(&registry), 0);

    let registration = register(&registry, &names, MAX_TIMEOUT)?;
    registry.terminate();
    let outcome = collect::<TestCodec, _, TestCodecError>(registration, &names, pending()).await;
    if !matches!(outcome, Err(RequestError::ShuttingDown)) {
        bail!("shutdown did not fail the request");
    }
    assert_eq!(pending_len(&registry), 0);
    Ok(())
}

fn run_arrivals(trace: ArrivalTrace) -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let pool: Vec<_> = trace.awaited.iter().map(|index| POOL[*index]).collect();
        let awaited = names(&pool)?;
        let outsider = names(&["not-awaited"])?;
        let registry = registry();
        let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
        let mut expected = vec![None; awaited.len()];

        for (raw, value) in trace.arrivals {
            let position = usize::from(raw) % (awaited.len() + 1);
            let (name, disposition) = if position == awaited.len() {
                (&outsider[0], ResponseDisposition::UnknownRequest)
            } else if expected[position].is_some() {
                (&awaited[position], ResponseDisposition::UnknownRequest)
            } else {
                expected[position] = Some(value);
                (&awaited[position], ResponseDisposition::Accepted)
            };
            assert_eq!(
                registry.accept(success(registration.id(), name, value)?),
                disposition
            );
        }

        let results =
            collect::<TestCodec, _, TestCodecError>(registration, &awaited, pending()).await?;
        assert_eq!(
            results,
            awaited
                .into_iter()
                .zip(expected)
                .map(|(name, value)| (
                    name,
                    match value {
                        Some(value) => Ok(value),
                        None => Err(ResponseError::Timeout),
                    }
                ))
                .collect()
        );
        assert_eq!(pending_len(&registry), 0);
        Ok(())
    })
}
