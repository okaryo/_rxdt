# Switching To The Latest Stream

`switchLatest` flattens a `Stream<Stream<T>>` by forwarding events only from
the most recently emitted inner stream.

## Public API

```dart
extension RxdtSwitchLatestStreamExtensions<T> on Stream<Stream<T>> {
  Stream<T> switchLatest();
}
```

The outer stream emits streams rather than ordinary values:

```text
outer: inner A -------- inner B -------- done
```

The output first listens to A. When B arrives, A becomes stale, its
subscription is canceled, and B becomes the only inner source allowed to emit
downstream.

## Switching And Cleanup

This implementation pauses the outer subscription while changing inner
subscriptions:

```text
outer emits B
  -> mark A stale
  -> pause outer
  -> cancel A
  -> await A cleanup
  -> listen to B
  -> resume outer
```

Waiting for cleanup prevents two inner resources from being owned at once.
Marking A stale before cancellation also prevents a queued late event from A
from reaching output.

If the outer stream emits more inners while paused, its subscription buffers
those events. They are handled sequentially after the current switch finishes.

## Required State

Unlike `mergeWith`, switching cannot treat all active sources equally. It must
remember:

- the outer subscription;
- the current inner subscription;
- whether outer is done;
- whether the current inner is active;
- whether a switch cleanup is in progress;
- a generation number identifying which inner is latest.

Each inner callback captures its generation. After a newer inner increments
the number, callbacks from an older generation are ignored.

## Completion

Outer done does not immediately close output when the latest inner remains
active:

```text
outer done + latest inner active -> wait
latest inner done                -> output done
```

If outer completes without ever emitting an inner, output closes immediately.
If an inner completes while outer remains open, output waits for a possible
future inner.

## Comparison With Concat And Merge

```text
concat: one active source; current must complete before next is subscribed
merge:  every source remains active concurrently
switch: one inner is selected; a new inner cancels the previous one
```

Switching is therefore stateful subscription selection, not just event
forwarding.
