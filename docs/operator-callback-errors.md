# Operator Callback Errors

`tap`, `mapValue`, `filterValue`, and `distinctValue` all execute synchronous
user callbacks while handling a source data event. Those callbacks can throw.
This project gives them one common contract:

```text
source data event
  -> operator callback throws
  -> triggering data is not forwarded
  -> downstream error event with the thrown object and stack trace
  -> later source events can still be processed
```

For example, if a mapper throws for `2`, this source sequence:

```text
data: 1
data: 2  -> mapper throws
data: 3
done
```

becomes:

```text
data: mapped 1
error
data: mapped 3
done
```

## Why Catch Inside The Operator

Each data handler catches only its user callback:

```dart
try {
  converted = convert(event);
} catch (error, stackTrace) {
  sink.addError(error, stackTrace);
  return;
}

sink.add(converted);
```

The exception is converted into an ordinary stream error event by
`sink.addError`. It is not thrown synchronously back through the code that
produced the source event.

The `return` is important. It makes the failed callback replace that one data
event rather than produce both an error and data for it.

## An Error Event Does Not Close The Stream

Adding an error and closing a stream are separate actions. The operator does
not close its sink when a callback fails, so the source may deliver more data
and eventually its normal done event.

This is different from an uncaught exception escaping an `async*` generator.
Such an exception terminates that generator, which sends an error followed by
done. Here the callback exception is caught at the operator boundary, so the
source subscription remains active.

## Stateful Failure

`distinctValue` has an extra rule. Its equality callback compares the
remembered previous value with the next value:

```text
previous: 1
next:     2
equality callback throws
```

The result is an error event, but `previous` remains `1`. If another `2`
arrives, it is compared with `1` again. State changes only after a value passes
comparison and is emitted successfully.

## Source Errors And Callback Errors

Both ultimately reach downstream through `onError`, but they enter the
operator differently:

```text
source error
  -> forwarded unchanged by the transformer

callback exception while handling source data
  -> caught by the operator
  -> converted to an error event
```

In both cases, the original error object and a stack trace are preserved.
