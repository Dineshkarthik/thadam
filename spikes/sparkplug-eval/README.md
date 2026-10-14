# sparkplug-eval

Spike code for the [Sparkplug B and OPC UA PubSub evaluation](../../docs/design/spikes/sparkplug-b-and-opcua-pubsub.md). It is a standalone Cargo project; the root workspace excludes `spikes/`.

## Fetch the proto first

`build.rs` compiles `proto/sparkplug_b.proto`. That file is not committed: it is Eclipse Tahu's schema, licensed EPL-2.0, and whether Thadam can ship it is open question O-1 in the spike report. Fetch it before building:

```sh
mkdir -p proto
curl -fsSL -o proto/sparkplug_b.proto \
  https://raw.githubusercontent.com/eclipse-tahu/tahu/master/sparkplug_b/sparkplug_b.proto
cargo build --release
```

The shipped `thadam-sparkplug` crate has its own Thadam-authored proto and does not need this file.
