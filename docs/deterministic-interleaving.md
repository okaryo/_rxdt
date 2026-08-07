# Deterministic Interleaving Tests

Multi-source operators observe events in the order in which their source
subscriptions deliver them. Independent real producers do not provide one
global production order, so tests should establish the cross-source delivery
order instead of relying on whichever microtask happens to run first.

## Strategy 1: Synchronous Test Sources

A `StreamController(sync: true)` delivers an event to its listener during the
`add` call. When events are added outside listener callbacks, the test can use
the call sequence as an explicit delivery schedule:

```dart
first.add('A1');
second.add('B1');
first.add('A2');
second.add('B2');
```

This establishes:

```text
A1 -> B1 -> A2 -> B2
```

The dedicated tests feed that same schedule into two operators:

```text
mergeWith:
  A1, B1, A2, B2

combineLatestWith:
  A1            -> not ready
  B1            -> A1+B1
  A2            -> A2+B1
  B2            -> A2+B2
```

The synchronous controllers are test drivers. They do not imply that real
producers are synchronous or that unrelated asynchronous sources have a fixed
global order.

## Strategy 2: Delivery Acknowledgements

Tests using asynchronous controllers can instead emit one event, wait for a
listener-side `Completer`, then emit the next event:

```text
add A1 -> await received A1
add B1 -> await received B1
add A2 -> await received A2
```

This strategy is used by the main `mergeWith` test. It exercises asynchronous
delivery while ensuring that the next source event cannot overtake the event
currently under test.

## What Not To Assert

A test should not start independent producers and assert one arbitrary global
interleaving. Each source preserves its own local event order, but scheduling
decides how the two local sequences are woven together. Tests should either
control that schedule or assert only properties that hold for every valid
interleaving.
