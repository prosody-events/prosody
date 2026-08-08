use super::{
    HeaderRejection, ID_TEXT_LEN, RESPONSE_AWAITED_HEADER, RESPONSE_DEADLINE_HEADER,
    RESPONSE_NODE_HEADER, RESPONSE_REQUEST_ID_HEADER, RESPONSE_VERSION_HEADER, RequestDeadline,
    RequestTag, parse_request_tag,
};
use crate::response::RequestId;
use crate::router::NodeId;
use crate::subsystem::{SubsystemName, SubsystemNameError};
use crate::test_util::assert_distinct_labels;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use strum::VariantArray;

/// The one request id every case carries, and the bytes its text form must
/// parse to.
const REQUEST_ID_TEXT: &str = "01983b2a-7e40-7d11-9b52-c4f0a3d8e6b1";
const REQUEST_ID_BYTES: [u8; 16] = [
    0x01, 0x98, 0x3b, 0x2a, 0x7e, 0x40, 0x7d, 0x11, 0x9b, 0x52, 0xc4, 0xf0, 0xa3, 0xd8, 0xe6, 0xb1,
];

/// The one node id every case carries, and the bytes its text form must parse
/// to.
const NODE_ID_TEXT: &str = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6";
const NODE_ID_BYTES: [u8; 16] = [
    0xf8, 0x1d, 0x4f, 0xae, 0x7d, 0xec, 0x11, 0xd0, 0xa7, 0x65, 0x00, 0xa0, 0xc9, 0x1e, 0x6b, 0xf6,
];

const DEADLINE_MICROS: u64 = 1_700_000_000_000_000;
const DEADLINE_TEXT: &[u8] = b"1700000000000000";

/// A subsystem name whose own text contains a comma — the reason the awaited
/// header repeats instead of listing names in one value.
const COMMA_NAME: &str = "billing,ledger";

/// A name no case ever awaits.
const OUTSIDER: &str = "treasury";

/// The longest name a header may carry, and one byte past it.
const LONG_NAME: &str = concat!(
    "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa"
);
const OVERLONG_NAME: &str = concat!(
    "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa", "aaaaaaaa",
    "a"
);
const _: () = assert!(
    LONG_NAME.len() == SubsystemName::MAX_BYTES,
    "LONG_NAME must sit exactly on the bound"
);
const _: () = assert!(
    OVERLONG_NAME.len() == SubsystemName::MAX_BYTES + 1,
    "OVERLONG_NAME must sit one byte past the bound"
);

/// The names a generated case draws its awaited list from: two plain names, a
/// name containing a comma, a padded name (which both the header parser and
/// [`SubsystemName`] trim), and a name exactly on the length bound.
const VOCABULARY: [&str; 5] = ["billing", "ledger", COMMA_NAME, "  padded  ", LONG_NAME];

/// One deviation from a well-formed request, and the outcome it must produce.
///
/// The oracle below matches exhaustively over this enum, so a shape added here
/// cannot be left without a documented outcome.
#[derive(Clone, Copy, Debug, VariantArray)]
enum Mutation {
    /// No deviation: the responder is one of the awaited names.
    WellFormed,
    /// The responder's own name contains a comma.
    WellFormedCommaResponder,
    /// Well-formed, but every awaited name belongs to another responder.
    NotAwaited,
    /// The record reserves no header at all.
    NoReservedHeaders,
    DuplicateVersion,
    DuplicateRequestId,
    DuplicateNode,
    DuplicateDeadline,
    MissingVersion,
    MissingRequestId,
    MissingNode,
    MissingDeadline,
    MissingAwaited,
    /// Only the revision header survives.
    OnlyVersion,
    /// A revision this responder does not read the other headers under.
    UnsupportedRevision,
    UnparseableRevision,
    /// The supported revision written as `01`.
    RevisionLeadingZero,
    /// The supported revision written as `+1`.
    RevisionSigned,
    RevisionValueAbsent,
    /// The 32-character unhyphenated UUID form.
    IdSimpleForm,
    /// The braced UUID form, `{...}`.
    IdBracedForm,
    /// The URN UUID form, `urn:uuid:...`.
    IdUrnForm,
    IdTruncated,
    IdNonUtf8,
    IdValueAbsent,
    DeadlineLeadingZero,
    DeadlineNonNumeric,
    DeadlineOverflow,
    DeadlineValueAbsent,
    AwaitedBlank,
    AwaitedNonUtf8,
    AwaitedTooLong,
    AwaitedValueAbsent,
    /// More awaited names than the former artificial cap allowed.
    ManyAwaited,
}

/// One record's reserved headers, plus which awaited name this responder
/// answers for and how far the header vector is rotated.
///
/// The responder is drawn from the awaited list by index, so the match lands in
/// every position — first, last and between — across a run.
#[derive(Clone, Debug)]
struct Case {
    mutation: Mutation,
    awaited: Vec<&'static str>,
    responder: usize,
    rotation: usize,
}

impl Arbitrary for Case {
    fn arbitrary(g: &mut Gen) -> Self {
        let count = usize::arbitrary(g) % VOCABULARY.len() + 1;
        Self {
            mutation: *g
                .choose(Mutation::VARIANTS)
                .unwrap_or(&Mutation::WellFormed),
            awaited: (0..count)
                .map(|_| *g.choose(&VOCABULARY).unwrap_or(&"billing"))
                .collect(),
            responder: usize::arbitrary(g),
            rotation: usize::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let shorter = (self.awaited.len() > 1).then(|| {
            let mut case = self.clone();
            case.awaited.truncate(case.awaited.len() - 1);
            case
        });
        let unrotated = (self.rotation > 0).then(|| Self {
            rotation: 0,
            ..self.clone()
        });
        let first = (self.responder > 0).then(|| Self {
            responder: 0,
            ..self.clone()
        });
        Box::new(shorter.into_iter().chain(unrotated).chain(first))
    }
}

impl Case {
    /// The case every deterministic row is built from: three awaited names with
    /// the responder last, so a parse that stops at the first match reports the
    /// wrong answer.
    fn deterministic(mutation: Mutation) -> Self {
        Self {
            mutation,
            awaited: vec!["ledger", COMMA_NAME, "billing"],
            responder: 2,
            rotation: 0,
        }
    }

    /// The name this responder answers for.
    fn responder(&self) -> Result<SubsystemName, SubsystemNameError> {
        let name = match self.mutation {
            Mutation::NotAwaited => OUTSIDER,
            Mutation::WellFormedCommaResponder => COMMA_NAME,
            _ => self.awaited[self.responder % self.awaited.len()],
        };
        SubsystemName::try_new(name)
    }

    /// A valid request header set with one unrelated producer header.
    fn base_headers(&self) -> Vec<(String, Option<Vec<u8>>)> {
        // A producer's own header rides every record, so it belongs in the base
        // rather than in one case.
        let mut headers = vec![
            header(crate::SOURCE_SYSTEM_HEADER, b"upstream"),
            header(RESPONSE_VERSION_HEADER, b"2"),
            header(RESPONSE_REQUEST_ID_HEADER, REQUEST_ID_TEXT.as_bytes()),
            header(RESPONSE_NODE_HEADER, NODE_ID_TEXT.as_bytes()),
            header(RESPONSE_DEADLINE_HEADER, DEADLINE_TEXT),
        ];
        headers.extend(
            self.awaited
                .iter()
                .map(|name| header(RESPONSE_AWAITED_HEADER, name.as_bytes())),
        );
        headers
    }

    /// The record's headers, well-formed and then deviated by the mutation.
    fn headers(self) -> Vec<(String, Option<Vec<u8>>)> {
        let rotation = self.rotation;
        let headers = self.base_headers();
        let mut headers = self.mutate(headers);
        let len = headers.len();
        if len > 0 {
            headers.rotate_left(rotation % len);
        }
        headers
    }

    /// Applies this case's one deviation from a valid request.
    fn mutate(self, mut headers: Vec<(String, Option<Vec<u8>>)>) -> Vec<(String, Option<Vec<u8>>)> {
        match self.mutation {
            Mutation::WellFormed | Mutation::NotAwaited => {}
            Mutation::WellFormedCommaResponder => {
                headers.push(header(RESPONSE_AWAITED_HEADER, COMMA_NAME.as_bytes()));
            }
            Mutation::NoReservedHeaders => {
                headers.retain(|(key, _)| key == crate::SOURCE_SYSTEM_HEADER);
            }
            Mutation::DuplicateVersion => headers.push(header(RESPONSE_VERSION_HEADER, b"2")),
            Mutation::DuplicateRequestId => headers.push(header(
                RESPONSE_REQUEST_ID_HEADER,
                REQUEST_ID_TEXT.as_bytes(),
            )),
            Mutation::DuplicateNode => {
                headers.push(header(RESPONSE_NODE_HEADER, NODE_ID_TEXT.as_bytes()));
            }
            Mutation::DuplicateDeadline => {
                headers.push(header(RESPONSE_DEADLINE_HEADER, DEADLINE_TEXT));
            }
            Mutation::MissingVersion => drop_header(&mut headers, RESPONSE_VERSION_HEADER),
            Mutation::MissingRequestId => drop_header(&mut headers, RESPONSE_REQUEST_ID_HEADER),
            Mutation::MissingNode => drop_header(&mut headers, RESPONSE_NODE_HEADER),
            Mutation::MissingDeadline => drop_header(&mut headers, RESPONSE_DEADLINE_HEADER),
            Mutation::MissingAwaited => drop_header(&mut headers, RESPONSE_AWAITED_HEADER),
            Mutation::OnlyVersion => headers.retain(|(key, _)| key == RESPONSE_VERSION_HEADER),
            Mutation::UnsupportedRevision => {
                set_value(&mut headers, RESPONSE_VERSION_HEADER, Some(b"1".to_vec()));
            }
            Mutation::UnparseableRevision => {
                set_value(&mut headers, RESPONSE_VERSION_HEADER, Some(b"one".to_vec()));
            }
            Mutation::RevisionLeadingZero => {
                set_value(&mut headers, RESPONSE_VERSION_HEADER, Some(b"01".to_vec()));
            }
            Mutation::RevisionSigned => {
                set_value(&mut headers, RESPONSE_VERSION_HEADER, Some(b"+1".to_vec()));
            }
            Mutation::RevisionValueAbsent => {
                set_value(&mut headers, RESPONSE_VERSION_HEADER, None);
            }
            Mutation::IdSimpleForm | Mutation::IdBracedForm | Mutation::IdUrnForm => set_value(
                &mut headers,
                RESPONSE_REQUEST_ID_HEADER,
                Some(other_id_form(self.mutation).into_bytes()),
            ),
            Mutation::IdTruncated => set_value(
                &mut headers,
                RESPONSE_NODE_HEADER,
                Some(NODE_ID_TEXT.as_bytes()[..ID_TEXT_LEN - 1].to_vec()),
            ),
            Mutation::IdNonUtf8 => set_value(
                &mut headers,
                RESPONSE_REQUEST_ID_HEADER,
                Some(vec![0xff; ID_TEXT_LEN]),
            ),
            Mutation::IdValueAbsent => set_value(&mut headers, RESPONSE_NODE_HEADER, None),
            Mutation::DeadlineLeadingZero => set_value(
                &mut headers,
                RESPONSE_DEADLINE_HEADER,
                Some(b"01700000000000000".to_vec()),
            ),
            Mutation::DeadlineNonNumeric => set_value(
                &mut headers,
                RESPONSE_DEADLINE_HEADER,
                Some(b"tomorrow".to_vec()),
            ),
            Mutation::DeadlineOverflow => set_value(
                &mut headers,
                RESPONSE_DEADLINE_HEADER,
                Some(b"18446744073709551616".to_vec()),
            ),
            Mutation::DeadlineValueAbsent => {
                set_value(&mut headers, RESPONSE_DEADLINE_HEADER, None);
            }
            Mutation::AwaitedBlank => {
                set_value(&mut headers, RESPONSE_AWAITED_HEADER, Some(b"   ".to_vec()));
            }
            Mutation::AwaitedNonUtf8 => set_value(
                &mut headers,
                RESPONSE_AWAITED_HEADER,
                Some(vec![0xff, 0xfe]),
            ),
            Mutation::AwaitedTooLong => set_value(
                &mut headers,
                RESPONSE_AWAITED_HEADER,
                Some(OVERLONG_NAME.as_bytes().to_vec()),
            ),
            Mutation::AwaitedValueAbsent => {
                set_value(&mut headers, RESPONSE_AWAITED_HEADER, None);
            }
            Mutation::ManyAwaited => {
                let chosen = self.awaited[self.responder % self.awaited.len()];
                drop_header(&mut headers, RESPONSE_AWAITED_HEADER);
                headers.extend(
                    (0_usize..64).map(|_| header(RESPONSE_AWAITED_HEADER, OUTSIDER.as_bytes())),
                );
                headers.push(header(RESPONSE_AWAITED_HEADER, chosen.as_bytes()));
            }
        }
        headers
    }
}

/// The request id in a text form the protocol does not accept.
///
/// Each one parses to the same 16 bytes, so only the length gate refuses it.
/// That gate is what makes one id have one text form.
fn other_id_form(mutation: Mutation) -> String {
    match mutation {
        Mutation::IdBracedForm => format!("{{{REQUEST_ID_TEXT}}}"),
        Mutation::IdUrnForm => format!("urn:uuid:{REQUEST_ID_TEXT}"),
        _ => REQUEST_ID_TEXT.replace('-', ""),
    }
}

/// What the parser must answer, read off the mutation alone — never re-derived
/// from the headers the way the parser derives it.
fn expected(mutation: Mutation) -> Result<Option<RequestTag>, HeaderRejection> {
    match mutation {
        Mutation::WellFormed | Mutation::WellFormedCommaResponder | Mutation::ManyAwaited => {
            Ok(Some(RequestTag::new(
                RequestId::from_bytes(REQUEST_ID_BYTES),
                NodeId::from_bytes(NODE_ID_BYTES),
                RequestDeadline::from_unix_micros(DEADLINE_MICROS),
            )))
        }
        Mutation::NotAwaited | Mutation::NoReservedHeaders => Ok(None),
        Mutation::DuplicateVersion
        | Mutation::DuplicateRequestId
        | Mutation::DuplicateNode
        | Mutation::DuplicateDeadline => Err(HeaderRejection::DuplicateSingleton),
        Mutation::MissingVersion
        | Mutation::MissingRequestId
        | Mutation::MissingNode
        | Mutation::MissingDeadline
        | Mutation::MissingAwaited
        | Mutation::OnlyVersion => Err(HeaderRejection::MissingSingleton),
        Mutation::UnsupportedRevision
        | Mutation::UnparseableRevision
        | Mutation::RevisionLeadingZero
        | Mutation::RevisionSigned
        | Mutation::RevisionValueAbsent => Err(HeaderRejection::UnsupportedVersion),
        Mutation::IdSimpleForm
        | Mutation::IdBracedForm
        | Mutation::IdUrnForm
        | Mutation::IdTruncated
        | Mutation::IdNonUtf8
        | Mutation::IdValueAbsent => Err(HeaderRejection::MalformedId),
        Mutation::DeadlineLeadingZero
        | Mutation::DeadlineNonNumeric
        | Mutation::DeadlineOverflow
        | Mutation::DeadlineValueAbsent => Err(HeaderRejection::MalformedDeadline),
        Mutation::AwaitedBlank
        | Mutation::AwaitedNonUtf8
        | Mutation::AwaitedTooLong
        | Mutation::AwaitedValueAbsent => Err(HeaderRejection::MalformedAwaited),
    }
}

/// Every mutation reports the outcome its variant documents, over a fixed
/// three-name awaited list whose match sits last.
#[test]
fn every_mutation_reports_its_documented_outcome() -> color_eyre::Result<()> {
    for &mutation in Mutation::VARIANTS {
        let parsed = parse(Case::deterministic(mutation))?;
        assert_eq!(parsed, expected(mutation), "{mutation:?} was misread");
    }
    Ok(())
}

/// The outcome depends on the header set, never on the order the producer wrote
/// it in or on where in the awaited list the responder's own name sits.
///
/// Each case carries exactly one defect, which is the whole of the guarantee
/// [`parse_request_tag`] states: a record with two defects is answered the same
/// way whatever the order, and only the reason it reports is the first one met.
#[test]
fn header_order_and_awaited_position_do_not_change_the_outcome() {
    fn property(case: Case) -> TestResult {
        let (expectation, description) = (expected(case.mutation), format!("{case:?}"));
        match parse(case) {
            Ok(parsed) if parsed == expectation => TestResult::passed(),
            Ok(parsed) => TestResult::error(format!(
                "{description} parsed to {parsed:?}, expected {expectation:?}"
            )),
            Err(error) => TestResult::error(format!("{description} could not be built: {error:#}")),
        }
    }

    QuickCheck::new().quickcheck(property as fn(Case) -> TestResult);
}

/// The names and value forms other processes write, frozen as literals: this is
/// the one test that must not read them from the constants it protects.
#[test]
fn canonical_headers_parse_to_their_ids() -> color_eyre::Result<()> {
    let responder = SubsystemName::try_new("billing")?;
    let expected = RequestTag::new(
        RequestId::from_bytes(REQUEST_ID_BYTES),
        NodeId::from_bytes(NODE_ID_BYTES),
        RequestDeadline::from_unix_micros(DEADLINE_MICROS),
    );

    for id_text in [
        "01983b2a-7e40-7d11-9b52-c4f0a3d8e6b1",
        "01983B2A-7E40-7D11-9B52-C4F0A3D8E6B1",
    ] {
        let headers = [
            ("response-version", Some(b"2".as_slice())),
            ("response-request-id", Some(id_text.as_bytes())),
            (
                "response-node",
                Some(b"f81d4fae-7dec-11d0-a765-00a0c91e6bf6".as_slice()),
            ),
            ("response-deadline", Some(DEADLINE_TEXT)),
            ("response-awaited", Some(b"ledger".as_slice())),
            ("response-awaited", Some(b"billing".as_slice())),
        ];

        assert_eq!(
            parse_request_tag(headers, &responder),
            Ok(Some(expected)),
            "{id_text} must parse to the frozen bytes"
        );
    }
    Ok(())
}

/// A responder answers for its own name and for no other, so a name that only
/// overlaps it addresses a different subsystem.
///
/// Every row is a well-formed request whose one awaited name is a near miss:
/// `billing` holds it, it holds `billing`, or it differs only in case. Each
/// refuses one weakening of the name match — a prefix, suffix or substring test
/// either way round, and a case-insensitive compare — that the generated cases
/// cannot reach, since they draw the responder from the awaited list itself.
#[test]
fn a_name_that_only_overlaps_the_responder_is_another_subsystem() -> color_eyre::Result<()> {
    let responder = SubsystemName::try_new("billing")?;

    for awaited in ["bill", "illing", "billings", "autobilling", "BILLING"] {
        let headers = [
            (RESPONSE_VERSION_HEADER, Some(b"2".as_slice())),
            (RESPONSE_REQUEST_ID_HEADER, Some(REQUEST_ID_TEXT.as_bytes())),
            (RESPONSE_NODE_HEADER, Some(NODE_ID_TEXT.as_bytes())),
            (RESPONSE_DEADLINE_HEADER, Some(DEADLINE_TEXT)),
            (RESPONSE_AWAITED_HEADER, Some(awaited.as_bytes())),
        ];

        assert_eq!(
            parse_request_tag(headers, &responder),
            Ok(None),
            "{awaited} is not this responder's name"
        );
    }
    Ok(())
}

/// Every rejection counts under its own label, so one rejection can never be
/// read as another in a dashboard.
#[test]
fn each_rejection_has_a_distinct_label() -> color_eyre::Result<()> {
    assert_distinct_labels(
        HeaderRejection::VARIANTS
            .iter()
            .map(|rejection| rejection.label()),
    )
}

/// Runs the parser over one case's headers.
fn parse(case: Case) -> color_eyre::Result<Result<Option<RequestTag>, HeaderRejection>> {
    let responder = case.responder()?;
    let headers = case.headers();
    Ok(parse_request_tag(
        headers
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_deref())),
        &responder,
    ))
}

fn header(key: &str, value: &[u8]) -> (String, Option<Vec<u8>>) {
    (key.to_owned(), Some(value.to_vec()))
}

fn drop_header(headers: &mut Vec<(String, Option<Vec<u8>>)>, key: &str) {
    headers.retain(|(present, _)| present != key);
}

/// Replaces the first occurrence of `key`'s value, leaving the header in place.
fn set_value(headers: &mut [(String, Option<Vec<u8>>)], key: &str, value: Option<Vec<u8>>) {
    if let Some(slot) = headers.iter_mut().find(|(present, _)| present == key) {
        slot.1 = value;
    }
}
