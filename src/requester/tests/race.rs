//! The delivery race: what one call waits for, and what it stops waiting for.
//!
//! Time is paused, so a virtual delay costs nothing and every elapsed
//! assertion is exact.

use super::{
    MAX_TIMEOUT, TestCodec, TestCodecError, TestError, names, poll_once, register, registry,
    success,
};
use crate::producer::ProducerError;
use crate::requester::collect::collect;
use crate::requester::{Outcome, RequestError, ResponseFailure};
use crate::response::ResponseDisposition;
use color_eyre::Result;
use color_eyre::eyre::bail;
use rdkafka::error::KafkaError;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::yield_now;
use tokio::time::{Instant, sleep};

/// Requests one registry in these suites admits.
const IN_FLIGHT: usize = 4;

/// Most subsystems one request here names.
const MAX_AWAITED: usize = 4;

/// How long the delivery report takes when a case makes it slow.
const REPORT_DELAY: Duration = Duration::from_secs(10);

/// How long a peer takes to answer when a case makes it late.
const ANSWER_DELAY: Duration = Duration::from_secs(1);

/// The failure a case gives the delivery report.
fn report_failure() -> ProducerError<TestCodecError> {
    ProducerError::Kafka(KafkaError::Canceled)
}

/// A complete response set returns at once, without waiting for the delivery
/// report.
#[tokio::test(start_paused = true)]
async fn a_complete_response_set_returns_before_the_report() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&["billing", "ledger"])?;
    let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
    let id = registration.id();
    let first = success(id, &awaited[0], 1)?;
    let second = success(id, &awaited[1], 2)?;
    let start = Instant::now();

    let produce = async {
        assert_eq!(registry.accept(first), ResponseDisposition::Accepted);
        assert_eq!(registry.accept(second), ResponseDisposition::Accepted);
        sleep(REPORT_DELAY).await;
        Ok::<(), ProducerError<TestCodecError>>(())
    };
    let outcomes = collect::<TestCodec, u32, TestError, _, TestCodecError>(
        &registration,
        produce,
        registration.deadline(),
    )
    .await?;

    assert_eq!(outcomes, vec![Outcome::Ok(1), Outcome::Ok(2)]);
    assert_eq!(
        Instant::now() - start,
        Duration::ZERO,
        "the call waited for a delivery report the responses had already made moot"
    );
    Ok(())
}

/// A failed delivery report with nothing accepted fails the call at once,
/// rather than at the deadline.
#[tokio::test(start_paused = true)]
async fn a_failed_report_with_no_response_fails_at_once() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&["billing"])?;
    let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
    let start = Instant::now();

    let produce = async { Err::<(), _>(report_failure()) };
    let outcome = collect::<TestCodec, u32, TestError, _, TestCodecError>(
        &registration,
        produce,
        registration.deadline(),
    )
    .await;

    let Err(RequestError::Produce(_)) = outcome else {
        bail!("a produce failure with no response must fail the call");
    };
    assert_eq!(
        Instant::now() - start,
        Duration::ZERO,
        "the call spent its deadline waiting for a response that could not come"
    );
    Ok(())
}

/// A failed delivery report after an accepted response is ignored, because the
/// response is the better evidence that the record landed.
#[tokio::test(start_paused = true)]
async fn a_failed_report_after_a_response_keeps_waiting() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&["billing", "ledger"])?;
    let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
    let id = registration.id();
    let first = success(id, &awaited[0], 3)?;
    let second = success(id, &awaited[1], 4)?;
    let start = Instant::now();

    let late = Arc::clone(&registry);
    let responder = tokio::spawn(async move {
        sleep(ANSWER_DELAY).await;
        late.accept(second)
    });
    let produce = async {
        assert_eq!(registry.accept(first), ResponseDisposition::Accepted);
        Err::<(), _>(report_failure())
    };
    let outcomes = collect::<TestCodec, u32, TestError, _, TestCodecError>(
        &registration,
        produce,
        registration.deadline(),
    )
    .await?;

    assert_eq!(responder.await?, ResponseDisposition::Accepted);
    assert_eq!(outcomes, vec![Outcome::Ok(3), Outcome::Ok(4)]);
    assert_eq!(
        Instant::now() - start,
        ANSWER_DELAY,
        "the call stopped waiting when the report failed"
    );
    Ok(())
}

/// A response that lands after the call has parked, and before the failed
/// report is observed, still reaches the caller.
///
/// One of the two subsystems stays silent, so the failed report meets a
/// request that is still open and already holds one answer. That is the one
/// state the emptiness test decides: a single awaited subsystem would end the
/// request on arrival and never reach the test at all.
#[tokio::test(start_paused = true)]
async fn a_response_racing_the_report_is_not_discarded() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&["billing", "ledger"])?;
    let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
    let answer = success(registration.id(), &awaited[0], 5)?;
    let start = Instant::now();

    let produce = async {
        yield_now().await;
        Err::<(), _>(report_failure())
    };
    let mut call = pin!(collect::<TestCodec, u32, TestError, _, TestCodecError>(
        &registration,
        produce,
        registration.deadline(),
    ));
    assert!(
        poll_once(call.as_mut()).await.is_pending(),
        "the call must park while the report is still outstanding"
    );

    assert_eq!(registry.accept(answer), ResponseDisposition::Accepted);
    assert_eq!(
        call.await?,
        vec![Outcome::Ok(5), Outcome::Failed(ResponseFailure::Timeout)]
    );
    assert_eq!(
        Instant::now() - start,
        MAX_TIMEOUT,
        "the failed report ended a request that had already accepted an answer"
    );
    Ok(())
}

/// A successful delivery report changes nothing: the call still waits for its
/// responses or its deadline.
#[tokio::test(start_paused = true)]
async fn a_successful_report_keeps_waiting() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&["billing", "ledger"])?;
    let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
    let start = Instant::now();

    let produce = async { Ok::<(), ProducerError<TestCodecError>>(()) };
    let outcomes = collect::<TestCodec, u32, TestError, _, TestCodecError>(
        &registration,
        produce,
        registration.deadline(),
    )
    .await?;

    assert_eq!(
        outcomes,
        vec![
            Outcome::Failed(ResponseFailure::Timeout),
            Outcome::Failed(ResponseFailure::Timeout),
        ]
    );
    assert_eq!(
        Instant::now() - start,
        MAX_TIMEOUT,
        "the call must wait out its deadline after a successful report"
    );
    Ok(())
}

/// Shutdown fails the call rather than returning the answers it had already
/// collected.
#[tokio::test(start_paused = true)]
async fn shutdown_discards_partial_results() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&["billing", "ledger"])?;
    let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
    let answer = success(registration.id(), &awaited[0], 6)?;
    assert_eq!(registry.accept(answer), ResponseDisposition::Accepted);

    let produce = async { Ok::<(), ProducerError<TestCodecError>>(()) };
    let mut call = pin!(collect::<TestCodec, u32, TestError, _, TestCodecError>(
        &registration,
        produce,
        registration.deadline(),
    ));
    assert!(
        poll_once(call.as_mut()).await.is_pending(),
        "the call must park while one subsystem is still unanswered"
    );

    registry.shutdown().await;
    let Err(RequestError::ShuttingDown) = call.await else {
        bail!("shutdown must fail the call rather than return one answer");
    };
    Ok(())
}
