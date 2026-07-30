# Stateless Map Value Operator

`mapValue` is a deliberately small reimplementation of the data path already
provided by Dart's built-in `Stream.map`. Its purpose is to study a stateless,
type-changing transformer rather than add missing production functionality.

## Public API

```dart
extension RxdtMapStreamExtensions<T> on Stream<T> {
  Stream<R> mapValue<R>(R Function(T event) convert);
}
```

The input and output types may differ:

```dart
final Stream<String> mapped = Stream.fromIterable([
  1,
  2,
]).mapValue((value) => 'value:$value');
```

## Data Path

Each data event is handled independently:

```text
source data: T
  -> convert(T)
  -> result: R
  -> downstream data: R
```

The transformer does not need to remember a previous value, a count, or any
other event history. It is therefore stateless.

```dart
handleData: (event, sink) {
  R converted;

  try {
    converted = convert(event);
  } catch (error, stackTrace) {
    sink.addError(error, stackTrace);
    return;
  }

  sink.add(converted);
}
```

Errors and done are not transformed. The default handlers supplied by
`StreamTransformer.fromHandlers` forward them, including the original error
object and stack trace.

Like `tap`, `mapValue` preserves the source stream kind:

```text
single-subscription source -> single-subscription result
broadcast source           -> broadcast result
```

For a broadcast source, conversion runs independently for each downstream
subscription. The operator does not introduce sharing.

If `convert` throws, the triggering data event is replaced by an error event
with the callback's stack trace. Later source events can still be converted.
The common contract is described in `operator-callback-errors.md`. A later
comparison will show how this learning implementation relates to Dart's
built-in `Stream.map`.
