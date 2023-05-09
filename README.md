Rabbit auto uses https://github.com/CleverCloud/lapin.  
The library provides a consumer and a publisher, which after the first successful connection run until manually closed. In case of loosing connection to RabbitMQ, they wait until the connection is reestablished without failing.  

The current async runtime used is tokio, but it can be easily extended (in the library code) to use different ones.

The version 0.3.0 has a bug which stops automatic reconnection.
There is some deadlock, which I was not able to track, so I have refactored the whole process, and kept most of the interfaces unchanged.
But the `configure` function has changed, and it is no longer async function. 
At the moment the latest version only supports tokio.
Config requires FullExecutor trait and Reactor trait.
Using tokio-reactor-trait and tokio-executor-trait makes this easy.


```rust

use rabbit_auto::publisher::{Serialise, PublishWrapper, simple_confirm_options};
use rabbit_auto::comms::Comms;
use rabbit_auto::config::Config;

/// If the configure is not called, the library will kill the app using `std::process::exit()`
/// The configure function is no longer async!
Comms::configure(Config::new(
    "app-tag", // the tag of the application for the rabbitmq
    "rabbit-host-ip",
    "rabbit user",
    "rabbit password",
    // reconnection interval when the connection is lost
    Duration::from_secs(3),
    tokio_executor_trait::Tokio::current(),
    tokio_reactor_trait::Tokio,
);

// Publisher Sink which expects the MsgOut and routing key String
// the created publisher of type impl Sink<(MsgOut, String)> + Unpin,
let publisher = PublishWrapper::<(MsgOut, String), _>::with_dynamic_key(
    "the exchange",
    Some(simple_confirm_options()),
    None, // basic publish options callback
    None, // basic publish properties callback 
).await;

// Publisher Sink which expects the MsgOut
// the created publisher of type impl Sink<MsgOut> + Unpin,
let publisher = PublishWrapper::<MsgOut, _>::with_static_key(
    "the exchange",
    "the routing key",
    Some(simple_confirm_options()),
    None, // basic publish options callback
    None, // basic publish properties callback 
).await;

// Creates a consumer which returns a Stream of items and auto ack objects.
// The item has to implement Deserialise trait so it is automatically deserialised.
// The auto ack object will automatically acks the delivery when the object drops.
let consumer = StreamBuilder {
        tag: "the app id",
        routing_key: "the routing key",
        exchange: "the exchange",
        ..StreamBuilder::default()}
    .create_auto_ack().await;
```