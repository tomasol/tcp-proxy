use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

#[derive(Clone)]
struct Item {
    address: String,
    secs_since_epoch: Arc<AtomicU64>,
}

pub struct TargetStore {
    epoch: Instant,
    quarantine_duration: Duration,
    inner: Vec<Item>,
    current_idx: AtomicUsize,
}

impl TargetStore {
    pub fn new(targets: Vec<String>, quarantine_duration: Duration) -> Self {
        assert!(!targets.is_empty());
        let epoch = Instant::now();
        TargetStore {
            epoch,
            quarantine_duration,
            inner: targets
                .into_iter()
                .map(|address| Item {
                    address,
                    secs_since_epoch: Arc::new(AtomicU64::new(0)),
                })
                .collect(),
            current_idx: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> Option<TargetHandler> {
        let now = Instant::now();
        for _ in 0..self.inner.len() {
            let idx = self.current_idx.fetch_add(1, SeqCst) % self.inner.len(); // wraps around on overflow
            let item = self.inner.get(idx).expect("must exist");
            if self.epoch + Duration::from_secs(item.secs_since_epoch.load(SeqCst)) <= now {
                return Some(TargetHandler::new(
                    self.epoch,
                    self.quarantine_duration,
                    item.clone(),
                ));
            }
        }
        None
    }

    /// Is there a target that is valid now?
    pub fn has_next(&self) -> bool {
        let now = Instant::now();
        self.inner
            .iter()
            .any(|item| self.epoch + Duration::from_secs(item.secs_since_epoch.load(SeqCst)) <= now)
    }
}

pub struct TargetHandler {
    epoch: Instant,
    quarantine_duration: Duration,
    succeeded_or_invalidated: AtomicBool,
    item: Item,
}

impl TargetHandler {
    fn new(epoch: Instant, quarantine_duration: Duration, item: Item) -> Self {
        // deprioritize while connecting
        let invalid_until = Instant::now() + quarantine_duration;
        let invalid_until = invalid_until.duration_since(epoch).as_secs();
        item.secs_since_epoch.store(invalid_until, SeqCst);
        Self {
            epoch,
            quarantine_duration,
            succeeded_or_invalidated: AtomicBool::new(false),
            item,
        }
    }

    pub fn mark_success(&self) {
        self.item.secs_since_epoch.store(0, SeqCst); // remove deprioritization
        self.succeeded_or_invalidated.store(true, SeqCst);
    }

    pub fn invalidate(&self) {
        let invalid_until = Instant::now() + self.quarantine_duration;
        let invalid_until = invalid_until.duration_since(self.epoch).as_secs();
        self.item.secs_since_epoch.store(invalid_until, SeqCst);
        self.succeeded_or_invalidated.store(true, SeqCst);
    }

    pub fn address(&self) -> &str {
        &self.item.address
    }
}

impl Drop for TargetHandler {
    fn drop(&mut self) {
        if !self.succeeded_or_invalidated.load(SeqCst) {
            warn!(
                "Detected possible connection timeout for {}",
                self.item.address
            );
            // connection probably failed, invalidate
            self.invalidate();
        }
    }
}
