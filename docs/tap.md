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

## Error And Done Forwarding

The transformer defines only `handleData`. Because it does not define
`handleError` or `handleDone`, `StreamTransformer.fromHandlers` forwards those
events without changing them.

The lifecycle test sends this source sequence:

```text
data: 1
error
data: 2
done
```

The downstream listener receives the same sequence. The error object and stack
trace retain their identities, and done is delivered exactly once. The tap
callback sees only the two data values because the current public API accepts
only an `onData` callback.

This also demonstrates that an error event is not necessarily terminal. The
source remains open after its error and can add another data event before
closing.

## Lazy Subscription

Calling `tap` binds a transformer and returns a new stream, but it does not
subscribe to the source:

```dart
final tapped = source.tap(onData);
```

The test observes the source controller's `onListen` callback. Its count remains
zero after the transformed stream is created. Only a downstream operation such
as `tapped.listen(...)` or `tapped.drain()` causes the transformed stream to
subscribe upstream.

```text
source.tap(...)
  -> transformed Stream is created
  -> source listen count: 0

transformed.drain()
  -> transformed Stream is listened to
  -> transformer listens to source
  -> source listen count: 1
```

This separation is important because a stream operator should describe a
transformation without starting work before a consumer exists.

Callback failures still need a separate test before that behavior is considered
understood.
