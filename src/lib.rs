//!
//! Wrappers around RabbitMQ connection to indefinitely (after the first successful connection) consume,
//! or publish without any errors. If the connection disconnects, the consumer and publisher will wait
//! until the connection is established back again.
pub mod config;
pub mod comms;
pub mod consumer;

pub mod publisher;

pub mod auto_ack;

pub mod stream_builder;
