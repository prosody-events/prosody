use crate::timers::slab::SlabId;

#[derive(Clone, Debug, Default)]
pub struct ContiguousRange {
    max_owned: Option<SlabId>,
    max_loading: Option<SlabId>,
    deleting: bool,
}

impl ContiguousRange {
    pub fn is_owned(&self, slab_id: SlabId) -> bool {
        self.max_owned.filter(|max| slab_id <= *max).is_some()
    }

    pub fn is_busy(&self, slab_id: SlabId) -> bool {
        self.is_loading(slab_id) || self.is_deleting(slab_id)
    }

    fn is_loading(&self, slab_id: SlabId) -> bool {
        self.max_loading.filter(|max| slab_id <= *max).is_some() && !self.is_owned(slab_id)
    }

    fn is_deleting(&self, slab_id: SlabId) -> bool {
        self.deleting && self.is_owned(slab_id)
    }

    pub fn start_load(&mut self, new_max: SlabId) {
        if self
            .max_owned
            .is_some_and(|current_max| current_max > new_max)
        {
            return;
        }

        self.max_loading = Some(new_max);
    }

    pub fn complete_load(&mut self) {
        let Some(new_max) = self.max_loading else {
            return;
        };

        self.max_owned = Some(new_max);
    }

    pub fn abort_load(&mut self) {
        self.max_loading = None;
    }

    pub fn start_delete(&mut self) {
        self.deleting = true;
    }

    pub fn end_delete(&mut self) {
        self.deleting = false;
    }
}
