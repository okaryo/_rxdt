# First Tap Operator

The first operator adds one behavior to Dart's standard `Stream<T>`:
observe each data event without changing the event sent downstream.

## Public API

The operator is exposed as an extension:

```dart
extension RxdtStreamExtensions<T> on Stream<T> {
  Stream<T> tap(void Function(T event) onData);
}
```

Both sides remain `Stream<T>`. A caller can therefore insert `tap` into an
ordinary Dart stream chain:

```dart
final values = await Stream.fromIterable([1, 2, 3])
    .tap(print)
    .toList();
```

## Data Path

The extension binds a private transformer to its source:

```text
source Stream<T>
  -> _TapStreamTransformer<T>
  -> onData(event)
  -> sink.add(event)
  -> downstream Stream<T>
```

The callback runs before the same event is added to the downstream sink. The
operator neither replaces the event nor changes its type.

The transformer currently uses `StreamTransformer.fromHandlers`. This keeps the
first implementation focused on the data path. That helper supplies the
subscription plumbing and the default forwarding behavior for handler types
that are not provided.

The current test deliberately covers only data observation and unchanged data
forwarding. Error forwarding, done forwarding, laziness, and callback failures
need separate tests before those behaviors are considered understood.
