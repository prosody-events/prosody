use crate::Key;
use crate::timers::datetime::CompactDateTime;
use educe::Educe;
use tracing::Span;

#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    pub key: Key,
    pub time: CompactDateTime,

    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    pub span: Span,
}