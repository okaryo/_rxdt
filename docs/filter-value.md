# Stateless Filter Value Operator

`filterValue` is a learning implementation of the filtering behavior already
provided by Dart's built-in `Stream.where`.

## Public API

```dart
extension RxdtFilterStreamExtensions<T> on Stream<T> {
  Stream<T> filterValue(bool Function(T event) test);
}
```

The predicate examines every data event. An accepted event is forwarded
unchanged, while a rejected event produces no downstream data event:

```text
source data
  -> test(data)
     -> true:  sink.add(data)
     -> false: no downstream event
```

For example:

```dart
final values = await Stream.fromIterable([
  1,
  2,
  3,
  4,
]).filterValue((value) => value.isEven).toList();

// [2, 4]
```

The operator stores no history between events. The decision for one value
depends only on that value and the predicate, so the transformer itself is
stateless.

Only data events are tested. Errors keep their original object and stack trace,
and done still closes the transformed stream. The operator also preserves the
source's single-subscription or broadcast kind.

For a broadcast source, the predicate runs once per downstream subscription,
just like the callback in `tap` and the conversion in `mapValue`.

An exception thrown by the predicate is deferred to the focused
callback-failure step. A later comparison will relate this learning operator to
Dart's built-in `Stream.where`.
