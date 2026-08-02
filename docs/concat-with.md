# Concatenating Two Streams

`concatWith` combines two streams sequentially. It subscribes to the first
stream immediately after downstream listens, then subscribes to the second
only after the first sends done.

## Public API

```dart
extension RxdtConcatStreamExtensions<T> on Stream<T> {
  Stream<T> concatWith(Stream<T> next);
}
```

The first implementation uses an asynchronous generator:

```dart
Stream<T> concatWith(Stream<T> next) async* {
  yield* this;
  yield* next;
}
```

Each `yield*` forwards one stream's data and error events and waits for its done
event before execution advances.

## Sequential Subscription

```text
downstream listen
  -> subscribe to first
  -> forward first events
  -> first done
  -> subscribe to second
  -> forward second events
  -> second done
  -> downstream done
```

Only one upstream subscription is active at a time. Merely creating the
concatenated stream listens to neither input.

The delayed second subscription matters for hot broadcast streams. Events that
the second stream emits before the first completes have no listener from
`concatWith` and can be lost.

## Error Is Not The Handoff Signal

An error from the first stream is forwarded with its stack trace, but it does
not start the second stream. Dart errors are not necessarily terminal:

```text
first data
first error  -> forward error, remain subscribed to first
first data
first done   -> now subscribe to second
```

The handoff is controlled by done, not error. A downstream listener using
`cancelOnError: true` may cancel the concatenated subscription instead; that
cancellation cleans up the currently active source and does not subscribe to
the next source. See `multi-source-lifecycle.md` for the ownership comparison
with `mergeWith`.

## Completion

First done changes the active source but does not close downstream. Downstream
done is emitted only after second done:

```text
first done  -> switch source
second done -> close output
```

## Initial Stream-Kind Choice

An `async*` function creates a single-subscription stream. Therefore this first
implementation returns a single-subscription result even when both inputs are
broadcast streams.

This is an explicit initial limitation, not a general rule of concatenation.
Preserving or defining broadcast behavior requires deciding whether each
downstream listener owns its own sequential input subscriptions, and will be
revisited with multi-stream lifecycle coordination.
