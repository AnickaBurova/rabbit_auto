use lapin::{
    acker::Acker,
    message::Delivery,
    options::{BasicAckOptions, BasicNackOptions},
    types::DeliveryTag,
};

// ── Action carried by AutoAckMulti ──────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum Action {
    Ack,
    Nack,
}

// ── Channel message ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum CommitAction {
    Ack(DeliveryTag),
    Nack(DeliveryTag),
}

// ── RAII guard returned to the caller ───────────────────────────────────────

/// Drop-based ack/nack guard for batched commits.
/// Behaves like `AutoAck`: defaults to ack on drop; call `change_to_nack` to
/// override before the guard goes out of scope, or `execute` for an explicit
/// async send.
pub struct AutoAckMulti {
    data: Option<Data>,
}

struct Data {
    tag: DeliveryTag,
    action: Action,
    sender: tokio::sync::mpsc::Sender<CommitAction>,
}

impl AutoAckMulti {

    pub fn empty() -> Self {
        Self { data: None }
    }

    fn build_action(action: Action, tag: DeliveryTag) -> CommitAction {
        match action {
            Action::Ack => CommitAction::Ack(tag),
            Action::Nack => CommitAction::Nack(tag),
        }
    }

    pub fn change_to_nack(&mut self) {
        if let Some(data) = self.data.as_mut() {
            data.action = Action::Nack;
        }
    }

    /// Explicitly send the commit decision now (async).
    pub async fn execute(&mut self) {
        if let Some(data) = self.data.take() {
            // tracing::error!(tag = data.tag, action = ?data.action, counter = data.counter, "Sending commit action");
            let action = Self::build_action(data.action, data.tag);
            let _ = data.sender.send(action).await;
        }
    }
}

impl Drop for AutoAckMulti {
    fn drop(&mut self) {
        if let Some(data) = self.data.take() {
            // tracing::error!(tag = data.tag, action = ?data.action, counter = data.counter, "Sending commit action in drop");
            let action = Self::build_action(data.action, data.tag);
            let sender = data.sender;
            tokio::spawn(async move {
                let _ = sender.send(action).await;
            });
        }
    }
}

// ── Per-entry bookkeeping ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
enum TagState {
    Pending,
    Acked,
    Nacked,
}

#[derive(Debug)]
struct Entry {
    tag: DeliveryTag,
    acker: Acker,
    state: TagState,
}

// ── Commits ──────────────────────────────────────────────────────────────────

/// Tracks in-flight deliveries and periodically flushes them in batches.
///
/// When `perform_auto_acking` is called it walks the front of the in-order
/// entry list, stopping at the first `Pending` entry.  Within the decided
/// prefix it groups consecutive same-action entries and issues a single
/// `basic_ack(multiple=true)` or `basic_nack(multiple=true)` per group,
/// using only the highest tag's `Acker` (which covers all lower tags on the
/// same channel).  All intermediate ackers are dropped without sending any
/// AMQP frame.
pub struct Commits {
    sender: tokio::sync::mpsc::Sender<CommitAction>,
    /// In-arrival order; new entries always appended at the back.
    entries: Vec<Entry>,
}

impl Commits {
    /// Returns the `Commits` and the paired receiver that delivers
    /// `CommitAction`s from `AutoAckMulti` guards.
    pub fn new() -> (Self, tokio::sync::mpsc::Receiver<CommitAction>) {
        let (sx, rx) = tokio::sync::mpsc::channel(8192);
        (
            Self {
                sender: sx,
                entries: Vec::new(),
            },
            rx,
        )
    }

    /// Registers an in-flight delivery.
    /// Returns an `AutoAckMulti` guard (for the downstream consumer) and the
    /// raw message payload.
    pub fn add_message(&mut self, delivery: Delivery) -> (AutoAckMulti, Vec<u8>) {
        let tag = delivery.delivery_tag;
        let data = delivery.data;
        self.entries.push(Entry {
            tag,
            acker: delivery.acker,
            state: TagState::Pending,
        });
        let guard = AutoAckMulti {
            data: Some(Data {
                tag,
                action: Action::Ack,
                sender: self.sender.clone(),
            }),
        };
        (guard, data)
    }

    /// Records an ack or nack decision sent by an `AutoAckMulti` guard.
    pub fn commit_tag(&mut self, action: CommitAction) {
        // tracing::warn!(action = ?action, "Committing action");
        let (tag, new_state) = match action {
            CommitAction::Ack(t) => (t, TagState::Acked),
            CommitAction::Nack(t) => (t, TagState::Nacked),
        };
        match self.entries.iter_mut().find(|e| e.tag == tag) {
            Some(entry) => {
                // tracing::info!(counter = entry.counter, "Committing action for counter");
                entry.state = new_state;
            }
            None => tracing::warn!("commit_tag: delivery tag {tag} not found in in-flight list"),
        }
    }

    /// Flushes all contiguous decided entries at the front of the queue.
    ///
    /// Consecutive acked entries become one `basic_ack(multiple=true)`;
    /// consecutive nacked entries become one `basic_nack(multiple=true, requeue=false)`.
    /// Processing stops at the first `Pending` entry.
    pub async fn perform_auto_acking(&mut self) -> anyhow::Result<()> {
        let committed_len = self
            .entries
            .iter()
            .take_while(|e| e.state != TagState::Pending)
            .count();

        if committed_len == 0 {
            return Ok(());
        }
        // tracing::error!(committed_len = committed_len, "Performing auto-acking");

        let to_process: Vec<Entry> = self.entries.drain(..committed_len).collect();
        let mut iter = to_process.into_iter().peekable();

        while iter.peek().is_some() {
            let is_ack = iter.peek().unwrap().state == TagState::Acked;

            // Walk the run, keeping only the last entry.  Every intermediate
            // Acker is implicitly dropped here — the final multi-ack/-nack
            // covers them all via AMQP's `multiple` flag.
            let mut last: Option<Entry> = None;
            while iter
                .peek()
                .map_or(false, |e| (e.state == TagState::Acked) == is_ack)
            {
                last = iter.next();
            }
            let last = last.unwrap(); // safe: the peek guard ensures at least one iteration

            if is_ack {
                // tracing::warn!("multi-ack up to delivery tag {}", last.tag);
                last.acker
                    .ack(BasicAckOptions { multiple: true })
                    .await
                    .map_err(|e| anyhow::anyhow!("multi-ack failed: {e}"))?;
            } else {
                // tracing::warn!("multi-nack up to delivery tag {}", last.tag);
                last.acker
                    .nack(BasicNackOptions {
                        multiple: true,
                        requeue: false,
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("multi-nack failed: {e}"))?;
            }
        }

        // tracing::error!(committed_len = committed_len, entries = self.entries.len(), "Auto-acking completed");

        Ok(())
    }
}
