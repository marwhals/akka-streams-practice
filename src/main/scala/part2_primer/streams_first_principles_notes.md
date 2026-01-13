# Streams - first principles notes

---

## Goal of Akka Streams library

- Asynchronous
- Backpressure
- Incremental
- Potentially infinite data processing systems a.k.a "Reactive Streams"

---

## Reactive Streams

https://www.reactive-streams.org/

Specification of async, backpressured stream

Concepts;
- publisher = emits elements (asynchronously)
- subscriber = receives elements
- processor = transforms elements along the way
- async
- backpressure

Reactive Streams is an SPI (Service Provider Interface), not an API. Focus here is the Akka Streams API

---

## Akka Streams

- Source = "publisher"
  - emits elements asynchronously
  - may or may not terminate
- Sink = "subscriber"
  - receives elements
  - terminates only when the publisher terminates
- Flow = "processor"
  - transforms elements
- We build streams by connecting components

### Directions
- upstream = to the source
- downstream = to the sink