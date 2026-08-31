//! This module holds [`TransactionBatch`] structure.

use {
    solana_time_utils::timestamp,
    std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError},
        },
        time::{Duration, Instant},
    },
    tokio_util::bytes::Bytes,
};

/// Batch of generated transactions timestamp is used to discard batches which
/// are too old to have valid blockhash.
#[derive(Clone)]
pub struct TransactionBatch {
    wired_transactions: Vec<WiredTransaction>,
    // Time of creation of this batch, used for batch timeouts
    timestamp: u64,
    completion: Option<Arc<BatchCompletion>>,
}

type WiredTransaction = Bytes;

const FANOUT_UNSET: usize = usize::MAX;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchSendReport {
    pub attempted_leaders: usize,
    pub successful_leaders: usize,
    pub failed_leaders: usize,
    pub elapsed: Duration,
}

pub struct TransactionBatchReceipt {
    first_write_receiver: Receiver<FirstWriteReport>,
    receiver: Receiver<BatchSendReport>,
}

impl TransactionBatchReceipt {
    pub fn recv_first_write_timeout(
        &self,
        timeout: Duration,
    ) -> Result<FirstWriteReport, RecvTimeoutError> {
        self.first_write_receiver.recv_timeout(timeout)
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<BatchSendReport, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    pub fn try_recv(&self) -> Result<BatchSendReport, TryRecvError> {
        self.receiver.try_recv()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FirstWriteReport {
    pub successful: bool,
    pub elapsed: Duration,
}

struct BatchCompletion {
    started: Instant,
    expected: AtomicUsize,
    completed: AtomicUsize,
    successful: AtomicUsize,
    first_write_sender: Mutex<Option<SyncSender<FirstWriteReport>>>,
    sender: Mutex<Option<SyncSender<BatchSendReport>>>,
}

impl BatchCompletion {
    fn new(
        first_write_sender: SyncSender<FirstWriteReport>,
        sender: SyncSender<BatchSendReport>,
    ) -> Self {
        Self {
            started: Instant::now(),
            expected: AtomicUsize::new(FANOUT_UNSET),
            completed: AtomicUsize::new(0),
            successful: AtomicUsize::new(0),
            first_write_sender: Mutex::new(Some(first_write_sender)),
            sender: Mutex::new(Some(sender)),
        }
    }

    fn prepare_fanout(&self, expected: usize) {
        if self
            .expected
            .compare_exchange(FANOUT_UNSET, expected, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && expected == 0
        {
            self.finish_first_write(false);
            self.finish();
        }
    }

    fn record(&self, successful: bool) {
        if successful {
            self.successful.fetch_add(1, Ordering::Relaxed);
            self.finish_first_write(true);
        }
        let completed = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        let expected = self.expected.load(Ordering::Acquire);
        if expected != FANOUT_UNSET && completed >= expected {
            if self.successful.load(Ordering::Acquire) == 0 {
                self.finish_first_write(false);
            }
            self.finish();
        }
    }

    fn finish_first_write(&self, successful: bool) {
        let Some(sender) = self
            .first_write_sender
            .lock()
            .expect("first write completion mutex")
            .take()
        else {
            return;
        };
        let _ = sender.try_send(FirstWriteReport {
            successful,
            elapsed: self.started.elapsed(),
        });
    }

    fn finish(&self) {
        let Some(sender) = self.sender.lock().expect("batch completion mutex").take() else {
            return;
        };
        let attempted = self.expected.load(Ordering::Acquire);
        let completed = self.completed.load(Ordering::Acquire).min(attempted);
        let successful = self.successful.load(Ordering::Acquire).min(completed);
        let _ = sender.try_send(BatchSendReport {
            attempted_leaders: attempted,
            successful_leaders: successful,
            failed_leaders: completed.saturating_sub(successful),
            elapsed: self.started.elapsed(),
        });
    }
}

impl IntoIterator for TransactionBatch {
    type Item = Bytes;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.wired_transactions.into_iter()
    }
}

impl TransactionBatch {
    pub fn new<T>(wired_transactions: Vec<T>) -> Self
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        let wired_transactions = wired_transactions
            .into_iter()
            .map(|v| Bytes::from_owner(v))
            .collect();

        Self {
            wired_transactions,
            timestamp: timestamp(),
            completion: None,
        }
    }

    pub fn new_tracked<T>(wired_transactions: Vec<T>) -> (Self, TransactionBatchReceipt)
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        let (first_write_sender, first_write_receiver) = mpsc::sync_channel(1);
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut batch = Self::new(wired_transactions);
        batch.completion = Some(Arc::new(BatchCompletion::new(first_write_sender, sender)));
        (
            batch,
            TransactionBatchReceipt {
                first_write_receiver,
                receiver,
            },
        )
    }

    pub(crate) fn prepare_fanout(&self, expected: usize) {
        if let Some(completion) = self.completion.as_ref() {
            completion.prepare_fanout(expected);
        }
    }

    pub(crate) fn report_worker_result(&self, successful: bool) {
        if let Some(completion) = self.completion.as_ref() {
            completion.record(successful);
        }
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl PartialEq for TransactionBatch {
    fn eq(&self, other: &Self) -> bool {
        self.wired_transactions == other.wired_transactions && self.timestamp == other.timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_batch_reports_only_after_every_fanout_target_finishes() {
        let (batch, receipt) = TransactionBatch::new_tracked(vec![vec![1_u8, 2, 3]]);
        batch.prepare_fanout(2);
        batch.report_worker_result(true);
        let first = receipt.first_write_receiver.try_recv().unwrap();
        assert!(first.successful);
        assert!(matches!(
            receipt.receiver.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        batch.report_worker_result(false);
        let report = receipt.receiver.try_recv().unwrap();
        assert_eq!(report.attempted_leaders, 2);
        assert_eq!(report.successful_leaders, 1);
        assert_eq!(report.failed_leaders, 1);
    }

    #[test]
    fn tracked_batch_with_no_leaders_finishes_as_failure() {
        let (batch, receipt) = TransactionBatch::new_tracked(vec![vec![1_u8]]);
        batch.prepare_fanout(0);
        let first = receipt.first_write_receiver.try_recv().unwrap();
        assert!(!first.successful);
        let report = receipt.receiver.try_recv().unwrap();
        assert_eq!(report.attempted_leaders, 0);
        assert_eq!(report.successful_leaders, 0);
    }
}
