use crate::timers::range::ContiguousRange;
use crate::timers::slab::SlabId;
use tokio::sync::watch::Sender;

pub struct LoadGuard<'a> {
    range_tx: &'a Sender<ContiguousRange>,
    completed: bool,
}

impl<'a> LoadGuard<'a> {
    pub fn new(range_tx: &'a Sender<ContiguousRange>, max_slab_id: SlabId) -> Self {
        range_tx.send_modify(|range| {
            range.start_load(max_slab_id);
        });
        Self {
            range_tx,
            completed: false,
        }
    }

    pub fn complete(mut self) {
        self.completed = true;
        self.range_tx.send_modify(ContiguousRange::complete_load);
    }
}

impl Drop for LoadGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.range_tx.send_modify(ContiguousRange::abort_load);
        }
    }
}

pub struct DeleteGuard<'a> {
    range_tx: &'a Sender<ContiguousRange>,
    completed: bool,
}

impl<'a> DeleteGuard<'a> {
    pub fn new(range_tx: &'a Sender<ContiguousRange>) -> Self {
        range_tx.send_modify(ContiguousRange::start_delete);
        Self {
            range_tx,
            completed: false,
        }
    }

    pub fn complete(mut self) {
        self.completed = true;
        self.range_tx.send_modify(ContiguousRange::end_delete);
    }
}

impl Drop for DeleteGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.range_tx.send_modify(ContiguousRange::end_delete);
        }
    }
}
