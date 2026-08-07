# Merging Two Streams

`mergeWith` combines two streams concurrently. When downstream listens, the
operator subscribes to both inputs and forwards events from whichever source
delivers next.

## Public API

```dart
extension RxdtMergeStreamExtensions<T> on Stream<T> {
  Stream<T> mergeWith(Stream<T> other);
}
```

## Concurrent Subscription

```text
downstream listen
  -> subscribe to first
  -> subscribe to second

first data  ----\
                 -> output data in arrival order
second data ----/
```

Unlike `concatWith`, the second source does not wait for first done. This means
a hot broadcast second source has a listener as soon as the merged output is
listened to. Events emitted before downstream listens can still be lost.

## Ordering

Each source's own event order is preserved:

```text
first:  1 before 2
second: 10 before 20
```

There is no inherent global order between independent sources. The focused
test makes interleaving deterministic by emitting one event and waiting for its
delivery before emitting the next:

```text
first 1 -> second 10 -> first 2 -> second 20
```

Real concurrent producers may produce any interleaving consistent with each
source's local order.

See `deterministic-interleaving.md` for two ways to control cross-source order
in tests without treating one arbitrary asynchronous schedule as guaranteed.

## Error And Completion

An error from either source is forwarded with its stack trace and does not
automatically stop the other source. The output closes only after both sources
send done:

```text
first done  -> output remains open
second data -> still forwarded
second done -> output done
```

## Subscription Ownership

The output `StreamController` owns two upstream subscriptions at the same time.
Its current wiring pauses, resumes, and cancels both subscriptions together.
Focused lifecycle tests verify that pause and resume reach both sources and
that downstream cancellation waits for both asynchronous cleanup futures. See
`multi-source-lifecycle.md` for the complete ownership comparison with
`concatWith`.

The initial controller is single-subscription, so `mergeWith` currently returns
a single-subscription stream even when both inputs are broadcast. Broadcast
semantics will be reconsidered after the multi-source lifecycle contract is
fully tested.
