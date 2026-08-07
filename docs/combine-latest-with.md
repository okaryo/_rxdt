# Combining Latest Values From Two Streams

`combineLatestWith` subscribes to two streams concurrently, remembers one
latest value from each source, and calls a combiner whenever either latest
value changes.

## Public API

```dart
extension RxdtCombineLatestStreamExtensions<A> on Stream<A> {
  Stream<R> combineLatestWith<B, R>(
    Stream<B> other,
    R Function(A first, B second) combine,
  );
}
```

## Readiness State

Receiving a value and being ready to emit are separate facts:

```text
first emits 1   -> remember first=1; no output yet
second emits 10 -> remember second=10; output combine(1, 10)
first emits 2   -> remember first=2; output combine(2, 10)
second emits 20 -> remember second=20; output combine(2, 20)
```

The implementation uses `hasFirst` and `hasSecond` flags in addition to
`late` latest-value fields. A null check would be incorrect because `A` or `B`
may itself be nullable and `null` can be a valid latest value.

The exact combined sequence depends on the cross-source delivery order. See
`deterministic-interleaving.md` for a focused test that controls one schedule
and contrasts this operator with `mergeWith`.

## Completion

The output closes after both sources complete. A completed source's latest
value remains usable while the other source continues:

```text
first emits 2, then done
second emits 20 -> combine(2, 20) is still possible
second done     -> output done
```

If one source completes without ever producing a value, no combination can
become ready. The current operator still waits for the other source to finish,
then closes without data. This matches the completion rule used by RxDart
0.28.0's `CombineLatestStream`.

## Errors

Source errors are forwarded without clearing either latest value. If the
combiner throws, that invocation becomes an error event and later source data
can trigger another combination. The new source value is already the latest
state when the combiner runs, so it remains available after a failed call.

## Subscription Ownership

Like `mergeWith`, this implementation owns two upstream subscriptions at the
same time. Downstream pause, resume, and cancellation are forwarded to both,
and cancellation waits for both source cleanups.

The initial output controller is single-subscription even when both inputs are
broadcast. Preserving broadcast behavior remains a later design decision.
