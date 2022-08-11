use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use futures::lock::Mutex;
use lapin::{Channel, ExchangeKind};
use lapin::options::{ExchangeDeclareOptions, ExchangeDeleteOptions};
use lapin::types::FieldTable;

pub type DeclareExchange = Pin<Box<dyn Fn(Arc<Mutex<Channel>>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync>>;



pub fn create_direct_exchange(exchange: String, options: Option<ExchangeDeclareOptions>, arguments: Option<FieldTable> ) -> DeclareExchange {
    Box::pin(move | channel|{
        let exchange = exchange.clone();
        let arguments = arguments.clone();
        let options = options.clone();
        Box::pin(async move {
            let channel = channel.lock().await;
            let backup_arguments = arguments.clone();
            let backup_options = options.clone();
            if let Err(err) = channel.exchange_declare(&exchange, ExchangeKind::Direct, options.unwrap_or_else(|| ExchangeDeclareOptions::default()), arguments.unwrap_or_else(|| FieldTable::default())).await {
                log::error!("Failed to declare exchange: {}", err);
                log::warn!("Deleting the old one and creating a new one");
                match channel.exchange_delete(&exchange, ExchangeDeleteOptions { if_unused: false, nowait: true}).await {
                    Ok(()) => {
                        if let Err(err) = channel.exchange_declare(&exchange, ExchangeKind::Direct, backup_options.unwrap_or_else(|| ExchangeDeclareOptions::default()), backup_arguments.unwrap_or_else(|| FieldTable::default())).await {
                            log::error!("Failed to declare exchange (Exiting): {}", err);
                            std::process::exit(1);
                        }
                    }
                    Err(err) => {
                        log::error!("Cannot delete the exchange to declare a new one (Exiting): {}", err);
                        std::process::exit(1);
                    }
                }
            }
            Ok(())
        })
    })
}