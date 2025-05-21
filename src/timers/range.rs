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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::datetime::CompactDateTime;

    #[test]
    fn test_default_local_range() {
        let range = LocalRange::default();

        // Default `owns` range should be empty
        assert_eq!(range.owns.start, CompactDateTime::MIN);
        assert_eq!(range.owns.end, CompactDateTime::MIN);

        // Default `loading` range should be `None`
        assert!(range.loading.is_none());
    }

    #[test]
    fn test_owns() {
        let start = CompactDateTime::from(1000u32);
        let end = CompactDateTime::from(2000u32);
        let range = LocalRange {
            owns: start..end,
            loading: None,
        };

        // Time within the range
        let time_in_range = CompactDateTime::from(1500u32);
        assert!(range.owns(time_in_range));

        // Time at the start of the range
        let time_at_start = start;
        assert!(range.owns(time_at_start));

        // Time at the end of the range (exclusive)
        let time_at_end = end;
        assert!(!range.owns(time_at_end));

        // Time outside the range
        let time_outside = CompactDateTime::from(2500u32);
        assert!(!range.owns(time_outside));
    }

    #[test]
    fn test_is_loading() {
        let start = CompactDateTime::from(3000u32);
        let end = CompactDateTime::from(4000u32);
        let range = LocalRange {
            owns: CompactDateTime::MIN..CompactDateTime::MIN,
            loading: Some(start..end),
        };

        // Time within the loading range
        let time_in_loading = CompactDateTime::from(3500u32);
        assert!(range.is_loading(time_in_loading));

        // Time at the start of the loading range
        let time_at_start = start;
        assert!(range.is_loading(time_at_start));

        // Time at the end of the loading range (exclusive)
        let time_at_end = end;
        assert!(!range.is_loading(time_at_end));

        // Time outside the loading range
        let time_outside = CompactDateTime::from(4500u32);
        assert!(!range.is_loading(time_outside));
    }

    #[test]
    fn test_is_loading_none() {
        let range = LocalRange {
            owns: CompactDateTime::MIN..CompactDateTime::MIN,
            loading: None,
        };

        // Any time should return false when `loading` is `None`
        let time = CompactDateTime::from(1234u32);
        assert!(!range.is_loading(time));
    }
}
