# Dart Built-In Operator Comparison

`mapValue`, `filterValue`, and `distinctValue` intentionally reproduce a small
part of behavior already provided by Dart's standard `Stream` API. Their value
in this project is making the implementation mechanics visible.

## Corresponding APIs

| Learning operator | Dart built-in | Data behavior |
| --- | --- | --- |
| `mapValue(convert)` | `map(convert)` | Replaces every data value |
| `filterValue(test)` | `where(test)` | Keeps only accepted data |
| `distinctValue(equals)` | `distinct(equals)` | Drops consecutive duplicates |
| `tap(onData)` | No direct equivalent | Observes data without changing it |

`tap` can be approximated with `map`:

```dart
stream.map((event) {
  onData(event);
  return event;
});
```

That works for the data path, but its name describes transformation rather than
observation. A dedicated `tap` makes the side effect explicit.

## Callback Failure Parity

Focused comparison tests feed the learning operator and its built-in
counterpart the same source sequence:

```text
data: 1
data: 2  -> callback throws
data: 3
done
```

`mapValue` and `Stream.map` both produce:

```text
data: converted 1
error
data: converted 3
done
```

`filterValue` and `Stream.where` both drop the failing `2`, emit an error for
it, and continue testing `3`.

`distinctValue` and `Stream.distinct` both keep their last successfully emitted
value when equality checking throws. Given `1, 2, 2`, with the first comparison
against `2` throwing, both produce:

```text
data: 1
error
data: 2
done
```

The second `2` is compared with `1` again. If the failed comparison had changed
the previous-value state to `2`, that second event would instead be suppressed.

## Structural Difference

Matching observable behavior does not require matching Dart's private
implementation classes:

- The standard library uses specialized internal stream classes for these
  operators.
- `mapValue` and `filterValue` use
  `StreamTransformer.fromHandlers`.
- `distinctValue` uses `Stream.eventTransformed` and a custom `EventSink` so
  its per-subscription state is visible.

The learning implementation favors explicit boundaries over the standard
library's internal optimization opportunities. Consumers should ordinarily use
Dart's built-in methods for these behaviors; the custom versions exist to study
how an operator can preserve the same contract.
