# Stateful Distinct Value Operator

`distinctValue` is the first operator in this project that must remember
something about an earlier data event.

## Public API

```dart
extension RxdtDistinctStreamExtensions<T> on Stream<T> {
  Stream<T> distinctValue([
    bool Function(T previous, T next)? equals,
  ]);
}
```

It suppresses consecutive duplicate data:

```text
source:     1, 1, 2, 2, 1, 1
downstream: 1,    2,    1
```

This is not global uniqueness. A value can be emitted again after a different
value appears.

## Required State

For each data event, the operator must know whether it has emitted a previous
value and, if so, what that value was:

```text
no previous value
  -> emit first value
  -> remember it

next value equals previous
  -> suppress it
  -> keep previous

next value differs
  -> emit it
  -> replace previous
```

The implementation uses both a boolean and a `late T` field:

```dart
var hasPrevious = false;
late T previous;
```

Using only `T? previous` would be ambiguous when `T` itself permits `null`:
`null` could mean either "no event yet" or "the previous event was null."

## One Event Sink Per Subscription

The mutable fields live in `_DistinctValueEventSink`, not in the reusable
transformer. `Stream.eventTransformed` invokes its sink factory when the
returned stream is listened to, creating a new sink for that subscription:

```text
transformer configuration
  -> first listen
     -> first event sink
        -> first previous-value state

  -> second listen
     -> second event sink
        -> second previous-value state
```

This structure avoids placing mutable event history on
`_DistinctValueStreamTransformer` itself.

Error events are forwarded without changing the remembered data value. In the
test sequence `data 1, error, data 1, data 2`, the second `1` is still a
duplicate, producing `data 1, error, data 2`. Done closes the transformed
stream.

The transformed stream also preserves the source's single-subscription or
broadcast kind.

A focused broadcast test will next verify that the two sink instances really
maintain independent histories. Custom equality and equality-callback failures
remain later learning steps.
