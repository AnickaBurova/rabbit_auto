//! Exchange declaration helpers.
//!
//! Pass the result of these functions to
//! [`Comms::declare_exchange`](crate::comms::Comms::declare_exchange).
//! The returned closure is stored and re-executed automatically on every
//! reconnect so the exchange topology is always consistent.
//!
//! If a declaration fails because an incompatible exchange with the same name
//! already exists, the library will delete the old exchange and redeclare it.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use lapin::{Channel, ExchangeKind};
pub use lapin::options::{ExchangeDeclareOptions, ExchangeDeleteOptions};
use lapin::types::FieldTable;

/// Type alias for the closure returned by the exchange helpers.
pub type DeclareExchange = Pin<Box<dyn Fn(Arc<Channel>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync>>;

/// Creates a declaration closure for an exchange of any kind.
///
/// Prefer the convenience wrappers [`create_direct_exchange`] and
/// [`create_topic_exchange`] for the common cases.
///
/// If the declaration fails (e.g. an incompatible exchange exists), the old
/// exchange is deleted and the declaration is retried.  A second failure is
/// fatal.
///
/// # Arguments
///
/// * `exchange` — exchange name.
/// * `kind` — [`ExchangeKind::Direct`], [`ExchangeKind::Topic`],
///   [`ExchangeKind::Fanout`], etc.
/// * `options` — AMQP declare options; `None` uses the lapin defaults
///   (`durable = false`, `auto_delete = false`, …).
/// * `arguments` — extra AMQP table arguments; `None` for none.
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::comms::Comms;
/// use rabbit_auto::exchanges::{create_exchange, ExchangeDeclareOptions};
/// use lapin::ExchangeKind;
///
/// # async fn example() {
/// Comms::declare_exchange(
///     "my-headers-exchange".to_string(),
///     create_exchange(
///         "my-headers-exchange".to_string(),
///         ExchangeKind::Headers,
///         Some(ExchangeDeclareOptions { durable: true, ..Default::default() }),
///         None,
///     ),
/// ).await;
/// # }
/// ```
pub fn create_exchange(exchange: String, kind: ExchangeKind, options: Option<ExchangeDeclareOptions>, arguments: Option<FieldTable> ) -> DeclareExchange {
      Box::pin(move | channel|{
        let exchange = exchange.clone();
        let arguments = arguments.clone();
        let options = options.clone();
          let kind = kind.clone();
        Box::pin(async move {
            let backup_arguments = arguments.clone();
            let backup_options = options.clone();
            if let Err(err) = channel.exchange_declare(&exchange, kind.clone(), options.unwrap_or_else(|| ExchangeDeclareOptions::default()), arguments.unwrap_or_else(|| FieldTable::default())).await {
                tracing::error!("Failed to declare exchange: {}", err);
                tracing::warn!("Deleting the old one and creating a new one");

                match channel.exchange_delete(&exchange, ExchangeDeleteOptions { if_unused: false, nowait: true}).await {
                    Ok(()) => {
                        if let Err(err) = channel.exchange_declare(&exchange, kind.clone(), backup_options.unwrap_or_else(|| ExchangeDeclareOptions::default()), backup_arguments.unwrap_or_else(|| FieldTable::default())).await {
                            tracing::error!("Failed to declare exchange (Exiting): {}", err);
                            std::process::exit(1);
                        }
                    }
                    Err(err) => {
                        tracing::error!("Cannot delete the exchange to declare a new one (Exiting): {}", err);
                        std::process::exit(1);
                    }
                }
            }
            Ok(())
        })
    })
}


/// Creates a declaration closure for a **direct** exchange.
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::comms::Comms;
/// use rabbit_auto::exchanges::create_direct_exchange;
///
/// # async fn example() {
/// Comms::declare_exchange(
///     "jobs".to_string(),
///     create_direct_exchange("jobs".to_string(), None, None),
/// ).await;
/// # }
/// ```
pub fn create_direct_exchange(exchange: String, options: Option<ExchangeDeclareOptions>, arguments: Option<FieldTable> ) -> DeclareExchange {
    create_exchange(exchange, ExchangeKind::Direct, options, arguments)
}

/// Creates a declaration closure for a **topic** exchange.
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::comms::Comms;
/// use rabbit_auto::exchanges::create_topic_exchange;
///
/// # async fn example() {
/// Comms::declare_exchange(
///     "events".to_string(),
///     create_topic_exchange("events".to_string(), None, None),
/// ).await;
/// # }
/// ```
pub fn create_topic_exchange(exchange: String, options: Option<ExchangeDeclareOptions>, arguments: Option<FieldTable> ) -> DeclareExchange {
    create_exchange(exchange, ExchangeKind::Topic, options, arguments)
}