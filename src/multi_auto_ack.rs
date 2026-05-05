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

/// RAII ack/nack guard used with the batched-commit consumer variant.
///
/// Obtained from the stream returned by
/// [`StreamBuilder::create_multi_auto_ack`](crate::stream_builder::StreamBuilder::create_multi_auto_ack).
///
/// Unlike [`AutoAck`](crate::auto_ack::AutoAck), this guard does **not** send
/// an AMQP frame immediately.  Instead it records the decision with the
/// associated [`Commits`] tracker.  On the next periodic tick, all contiguous
/// decided deliveries at the front of the in-flight list are flushed in a
/// single `basic_ack(multiple=true)` or `basic_nack(multiple=true)` call.
///
/// The default action is **ack**.  Call [`change_to_nack`](AutoAckMulti::change_to_nack)
/// before the guard is dropped to nack instead.  Call
/// [`execute`](AutoAckMulti::execute) for an explicit async send.
///
/// # Important
///
/// Batching is only as efficient as the order in which decisions arrive.
/// A single `Pending` delivery blocks all subsequent flushes until it is
/// decided, so ensure every `AutoAckMulti` is eventually resolved.
pub struct AutoAckMulti {
    data: Option<Data>,
}

struct Data {
    tag: DeliveryTag,
    action: Action,
    sender: tokio::sync::mpsc::Sender<CommitAction>,
}

impl AutoAckMulti {
    /// Creates a no-op guard that carries no delivery tag.
    ///
    /// Useful as a placeholder where an `AutoAckMulti` is required by type but
    /// no actual delivery is associated (e.g. in tests or sentinel values).
    pub fn empty() -> Self {
        Self { data: None }
    }

    fn build_action(action: Action, tag: DeliveryTag) -> CommitAction {
        match action {
            Action::Ack => CommitAction::Ack(tag),
            Action::Nack => CommitAction::Nack(tag),
        }
    }

    /// Switch the pending action from ack to **nack**.
    ///
    /// Has no effect on an [`empty`](AutoAckMulti::empty) guard or after
    /// [`execute`](AutoAckMulti::execute) has been called.
    pub fn change_to_nack(&mut self) {
        if let Some(data) = self.data.as_mut() {
            data.action = Action::Nack;
        }
    }

    /// Record the decision immediately rather than waiting for drop.
    ///
    /// The decision is sent to the [`Commits`] channel; the actual AMQP frame
    /// is still issued on the next [`Commits::perform_auto_acking`] tick.
    /// Subsequent drops are no-ops.
    pub async fn execute(&mut self) -> anyhow::Result<()> {
        if let Some(data) = self.data.take() {
            // tracing::error!(tag = data.tag, action = ?data.action, counter = data.counter, "Sending commit action");
            let action = Self::build_action(data.action, data.tag);
            data.sender.send(action).await?;
        }
        Ok(())
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

/// Batched ack/nack tracker paired with the multi-auto-ack consumer stream.
///
/// `Commits` is created internally by
/// [`StreamBuilder::create_multi_auto_ack`](crate::stream_builder::StreamBuilder::create_multi_auto_ack).
/// You only need to interact with it directly if you are building a custom
/// consumer loop.
///
/// ## How batching works
///
/// 1. Each incoming delivery is registered with [`add_message`](Commits::add_message),
///    which returns an [`AutoAckMulti`] guard for the caller and stores an entry
///    in the in-flight list.
/// 2. When a guard is dropped (or [`execute`](AutoAckMulti::execute) is called),
///    the decision (ack/nack) is sent back via an internal channel.
/// 3. [`commit_tag`](Commits::commit_tag) drains that channel and marks entries
///    as decided.
/// 4. [`perform_auto_acking`](Commits::perform_auto_acking) walks the front of
///    the in-flight list, stops at the first `Pending` entry, and issues one
///    `basic_ack(multiple=true)` or `basic_nack(multiple=true)` per consecutive
///    run of the same action.  All intermediate [`Acker`]s are dropped without
///    sending any AMQP frame — only the final one in each run is used.
///
/// ## Example — custom loop
///
/// ```rust,no_run
/// use rabbit_auto::multi_auto_ack::Commits;
/// use rabbit_auto::stream_builder::{StreamBuilder, Deserialise};
/// use futures::StreamExt;
/// use anyhow::Result;
///
/// struct Msg(Vec<u8>);
/// impl Deserialise for Msg {
///     fn deserialise(data: Vec<u8>) -> Result<Self> { Ok(Msg(data)) }
/// }
///
/// # async fn example() {
/// let mut consumer = StreamBuilder {
///     exchange: "events",
///     routing_key: "my.key",
///     tag: "batch-consumer",
///     ..Default::default()
/// }
/// .create_plain()
/// .await;
///
/// let (mut commits, mut action_rx) = Commits::new();
///
/// loop {
///     tokio::select! {
///         Some(delivery) = consumer.next() => {
///             let (guard, data) = commits.add_message(delivery);
///             // hand `guard` and `data` to your processing logic
///             drop(guard); // defaults to ack
///         }
///         Some(action) = action_rx.recv() => {
///             commits.commit_tag(action);
///         }
///     }
///     // call commits.perform_auto_acking().await periodically
/// }
/// # }
/// ```
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
