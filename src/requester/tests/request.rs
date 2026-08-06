//! What one call refuses before it produces anything, and what it puts on the
//! record when it does produce.

use super::{
    MAX_TIMEOUT, NODE, POOL, RequestPayload, TestError, distinct_indices, names, poll_once,
    registry, requester,
};
use crate::Topic;
use crate::requester::{
    HEADER_INLINE, Outcome, RequestError, ResponseFailure, append_request_headers,
};
use crate::response::RequestId;
use crate::response::headers::{
    ID_TEXT_LEN, RESERVED_REQUEST_HEADERS, RequestTag, parse_request_tag,
};
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::bail;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use smallvec::SmallVec;
use std::iter::{empty, once};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

/// Requests one registry in these suites admits.
const IN_FLIGHT: usize = 4;

/// Most subsystems one request here names.
const MAX_AWAITED: usize = 4;

/// Timeout the accepted cases ask for.
const TIMEOUT: Duration = Duration::from_secs(5);

/// The subsystem name no generated request awaits.
const STRANGER: &str = "not-awaited";

/// Header names a caller may supply.
const USER_NAMES: [&str; 3] = ["tenant", "correlation", "priority"];

/// Header values a caller may supply.
const USER_VALUES: [&str; 3] = ["alpha", "beta", "gamma"];

/// The subsystem sets and caller headers one request carries.
///
/// Every set names the comma-bearing subsystem, because a comma is legal in a
/// name and one joined `response-awaited` header could not be read back.
#[derive(Clone, Debug)]
struct HeaderTrace {
    /// Distinct pool indices beside the comma-bearing name.
    awaited: Vec<usize>,
    /// Caller headers, as indices into the name and value pools.
    user: Vec<(usize, usize)>,
}

impl Arbitrary for HeaderTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        // The last pool name carries a comma and is always awaited, so the
        // one-header-per-name rule is under test on every iteration.
        let extra = usize::arbitrary(g) % MAX_AWAITED;
        let awaited = distinct_indices(g, POOL.len() - 1, extra);
        let user = (0..usize::arbitrary(g) % (USER_NAMES.len() + 1))
            .map(|_| {
                (
                    usize::arbitrary(g) % USER_NAMES.len(),
                    usize::arbitrary(g) % USER_VALUES.len(),
                )
            })
            .collect();
        Self { awaited, user }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        if self.awaited.is_empty() && self.user.is_empty() {
            return Box::new(empty());
        }
        let mut shorter = self.clone();
        if shorter.user.pop().is_none() {
            shorter.awaited.pop();
        }
        Box::new(once(shorter))
    }
}

/// Every awaited subsystem reads its own tag back out of the emitted headers,
/// and a subsystem the request never named reads nothing.
#[quickcheck]
fn the_emitted_headers_parse_back(trace: HeaderTrace) -> TestResult {
    match run_headers(trace) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
}

/// A request with invalid arguments is refused before anything is registered
/// or produced.
#[tokio::test]
async fn invalid_arguments_are_refused_before_registration() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let requester = requester(Arc::clone(&registry))?;
    let topic = Topic::from("requests");
    let none = names(&[])?;
    let repeated = names(&["billing", "ledger", "billing"])?;
    let one = names(&["billing"])?;
    let over_cap = names(&POOL[..=MAX_AWAITED])?;
    let no_headers: Vec<(&'static str, &'static str)> = Vec::new();

    match requester
        .request(
            no_headers.clone(),
            topic,
            "key",
            RequestPayload,
            &none,
            TIMEOUT,
        )
        .await
    {
        Err(RequestError::NoSubsystems) => {}
        other => bail!("a request naming no subsystem must be refused, not {other:?}"),
    }

    match requester
        .request(
            no_headers.clone(),
            topic,
            "key",
            RequestPayload,
            &repeated,
            TIMEOUT,
        )
        .await
    {
        Err(RequestError::DuplicateSubsystem { name }) => {
            assert_eq!(name.as_str(), "billing");
        }
        other => bail!("a repeated subsystem must be refused, not {other:?}"),
    }

    match requester
        .request(
            no_headers.clone(),
            topic,
            "key",
            RequestPayload,
            &over_cap,
            TIMEOUT,
        )
        .await
    {
        Err(RequestError::TooManySubsystems { count, max }) => {
            assert_eq!((count, max), (MAX_AWAITED + 1, MAX_AWAITED));
        }
        other => bail!("a request over the awaited limit must be refused, not {other:?}"),
    }

    match requester
        .request(
            no_headers,
            topic,
            "key",
            RequestPayload,
            &one,
            MAX_TIMEOUT + TIMEOUT,
        )
        .await
    {
        Err(RequestError::TimeoutOutOfRange { .. }) => {}
        other => bail!("a timeout over the configured ceiling must be refused, not {other:?}"),
    }

    for reserved in RESERVED_REQUEST_HEADERS {
        match requester
            .request(
                vec![(reserved, "mine")],
                topic,
                "key",
                RequestPayload,
                &one,
                TIMEOUT,
            )
            .await
        {
            Err(RequestError::ReservedHeader { name }) => assert_eq!(name, reserved),
            other => bail!("the reserved header {reserved} must be refused, not {other:?}"),
        }
    }

    assert_eq!(
        registry.len(),
        0,
        "a refused request left a record in the registry"
    );
    Ok(())
}

/// One valid call holds a registry record before its record reaches Kafka,
/// answers one outcome per named subsystem, and then leaves the registry empty.
#[tokio::test(start_paused = true)]
async fn a_valid_call_registers_first_and_gives_its_record_back() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let requester = requester(Arc::clone(&registry))?;
    let awaited = names(&["billing", "ledger"])?;
    let no_headers: Vec<(&'static str, &'static str)> = Vec::new();

    let mut call = pin!(requester.request::<_, u32, TestError>(
        no_headers,
        Topic::from("requests"),
        "key",
        RequestPayload,
        &awaited,
        TIMEOUT,
    ));
    assert!(
        poll_once(call.as_mut()).await.is_pending(),
        "the call must park until a response or its deadline"
    );
    assert_eq!(
        registry.len(),
        1,
        "the record reached the producer before the registry could answer for it"
    );
    assert_eq!(
        registry.available_permits(),
        IN_FLIGHT - 1,
        "the call produced a record without taking an admission permit"
    );

    assert_eq!(
        call.await?,
        vec![
            Outcome::Failed(ResponseFailure::Timeout),
            Outcome::Failed(ResponseFailure::Timeout),
        ],
        "one unanswered outcome must come back per named subsystem"
    );
    assert_eq!(registry.len(), 0, "the finished call kept its map record");
    assert_eq!(
        registry.available_permits(),
        IN_FLIGHT,
        "the finished call kept its admission permit"
    );
    Ok(())
}

/// A call dropped before it finishes leaves no map record and no permit behind,
/// because the record belongs to the call rather than to the future's progress.
#[tokio::test(start_paused = true)]
async fn a_cancelled_call_leaves_the_registry_empty() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let requester = requester(Arc::clone(&registry))?;
    let awaited = names(&["billing", "ledger"])?;
    let no_headers: Vec<(&'static str, &'static str)> = Vec::new();

    // Boxed rather than pinned on the stack, so dropping the handle drops the
    // call itself. That drop is what this case is about.
    let mut call = Box::pin(requester.request::<_, u32, TestError>(
        no_headers,
        Topic::from("requests"),
        "key",
        RequestPayload,
        &awaited,
        TIMEOUT,
    ));
    assert!(
        poll_once(call.as_mut()).await.is_pending(),
        "the call must park until a response or its deadline"
    );
    assert_eq!(registry.len(), 1, "the call registered no record to cancel");

    drop(call);
    assert_eq!(registry.len(), 0, "a cancelled call kept its map record");
    assert_eq!(
        registry.available_permits(),
        IN_FLIGHT,
        "a cancelled call kept its admission permit"
    );
    Ok(())
}

/// Drives one generated header trace.
fn run_headers(trace: HeaderTrace) -> Result<()> {
    let HeaderTrace {
        awaited: chosen,
        user,
    } = trace;
    let mut pool: Vec<&str> = vec![POOL[POOL.len() - 1]];
    pool.extend(chosen.iter().map(|index| POOL[*index]));
    let awaited = names(&pool)?;

    let id = RequestId::new();
    let mut request_buf = [0_u8; ID_TEXT_LEN];
    let mut node_buf = [0_u8; ID_TEXT_LEN];
    let mut headers = SmallVec::<[(&'static str, &str); HEADER_INLINE]>::new();
    for (name, value) in &user {
        headers.push((USER_NAMES[*name], USER_VALUES[*value]));
    }
    append_request_headers(
        &mut headers,
        id,
        &mut request_buf,
        NODE,
        &mut node_buf,
        &awaited,
    );

    let wire: Vec<(&str, Option<&[u8]>)> = headers
        .iter()
        .map(|(name, value)| (*name, Some(value.as_bytes())))
        .collect();
    let expected = RequestTag::new(id, NODE);
    for name in &awaited {
        assert_eq!(
            parse_request_tag(wire.iter().copied(), name)?,
            Some(expected),
            "the awaited subsystem {name} did not read its own tag back"
        );
    }
    let stranger = SubsystemName::try_new(STRANGER)?;
    assert_eq!(
        parse_request_tag(wire.iter().copied(), &stranger)?,
        None,
        "a subsystem the request never named read a tag"
    );
    Ok(())
}
