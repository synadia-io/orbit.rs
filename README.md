<p align="center">
  <img src="orbit.png">
</p>

Orbit.rs is a set of independent utilities around NATS ecosystem that aims to
boost productivity and provide higher abstraction layer for NATS async rust client.
Note that these libraries will evolve rapidly and API guarantees are
not made until the specific project has a v1.0.0 version.

# Utilities

This is a list of the current utilities.

| Module | Description                                   | Docs                        | Version |
| ------ | --------------------------------------------- | --------------------------- | ------- |
| nats extra        | Set of useful tools for Core NATS | [README.md](nats-extra/README.md) | [![Crates.io](https://img.shields.io/crates/v/nats-extra.svg)](https://crates.io/crates/nats-extra) |
| jetstream extra   | Set of useful tools for NATS JetStream | [README.md](jetstream-extra/README.md) | [![Crates.io](https://img.shields.io/crates/v/jetstream-extra.svg)](https://crates.io/crates/jetstream-extra) |
| nats counters     | Distributed counters using JetStream | [README.md](counters/README.md) | [![Crates.io](https://img.shields.io/crates/v/nats-counters.svg)](https://crates.io/crates/nats-counters) |
