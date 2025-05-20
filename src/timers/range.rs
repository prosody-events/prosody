use crate::timers::datetime::CompactDateTime;
use std::ops::Range;

#[derive(Clone, Debug)]
pub struct LocalRange {
    owns: Range<CompactDateTime>,
    loading: Option<Range<CompactDateTime>>,
}

impl LocalRange {
    pub fn owns(&self, time: CompactDateTime) -> bool {
        self.owns.contains(&time)
    }

    pub fn is_loading(&self, time: CompactDateTime) -> bool {
        self.loading.as_ref().is_some_and(|r| r.contains(&time))
    }
}

impl Default for LocalRange {
    fn default() -> Self {
        Self {
            owns: CompactDateTime::MIN..CompactDateTime::MIN,
            loading: None,
        }
    }
}
