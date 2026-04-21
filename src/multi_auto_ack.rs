use lapin::{
    acker::Acker,
    message::Delivery,
    options::{BasicAckOptions, BasicNackOptions},
    types::DeliveryTag,
};

// ── Action carried by AutoAckMulti ──────────────────────────────────────────

#[derive(Clone, Copy)]
enum Action {
    Ack,
    Nack,
}

// ── Channel message ──────────────────────────────────────────────────────────

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
    tag: DeliveryTag,
    action: Action,
    sender: tokio::sync::mpsc::Sender<CommitAction>,
    sent: bool,
}

impl AutoAckMulti {
    fn build_action(&self) -> CommitAction {
        match self.action {
            Action::Ack => CommitAction::Ack(self.tag),
            Action::Nack => CommitAction::Nack(self.tag),
        }
    }

    pub fn change_to_nack(&mut self) {
        self.action = Action::Nack;
    }

    /// Explicitly send the commit decision now (async).
    pub async fn execute(&mut self) {
        if !self.sent {
            self.sent = true;
            let action = self.build_action();
            let _ = self.sender.send(action).await;
        }
    }
}

impl Drop for AutoAckMulti {
    fn drop(&mut self) {
        if !self.sent {
            self.sent = true;
            let action = self.build_action();
            // try_send is the only non-async option in Drop.
            // The channel is large (8192 slots) so this should never fail in
            // practice; if it does the delivery will simply be requeued on
            // channel closure.
            let _ = self.sender.try_send(action);
        }
    }
}

// ── Per-entry bookkeeping ────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum TagState {
    Pending,
    Acked,
    Nacked,
}

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
            tag,
            action: Action::Ack,
            sender: self.sender.clone(),
            sent: false,
        };
        (guard, data)
    }

    /// Records an ack or nack decision sent by an `AutoAckMulti` guard.
    pub fn commit_tag(&mut self, action: CommitAction) {
        let (tag, new_state) = match action {
            CommitAction::Ack(t) => (t, TagState::Acked),
            CommitAction::Nack(t) => (t, TagState::Nacked),
        };
        match self.entries.iter_mut().find(|e| e.tag == tag) {
            Some(entry) => entry.state = new_state,
            None => log::warn!("commit_tag: delivery tag {tag} not found in in-flight list"),
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
                log::warn!("multi-ack up to delivery tag {}", last.tag);
                last.acker
                    .ack(BasicAckOptions { multiple: true })
                    .await
                    .map_err(|e| anyhow::anyhow!("multi-ack failed: {e}"))?;
            } else {
                log::warn!("multi-nack up to delivery tag {}", last.tag);
                last.acker
                    .nack(BasicNackOptions {
                        multiple: true,
                        requeue: false,
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("multi-nack failed: {e}"))?;
            }
        }

        Ok(())
    }
}
