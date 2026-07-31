# Recover Value Operator

`recoverValue` is the first operator in this project that handles an error
instead of forwarding it unchanged.

## Public API

```dart
extension RxdtRecoverStreamExtensions<T> on Stream<T> {
  Stream<T> recoverValue(
    T Function(Object error, StackTrace stackTrace) recover,
  );
}
```

The callback receives the source error and its stack trace, then returns one
replacement value of the same type as the stream:

```dart
source.recoverValue((error, stackTrace) => fallbackValue);
```

## Event Path

Given this source sequence:

```text
data: 1
error: source failed
data: 2
done
```

returning `-1` from the recovery callback produces:

```text
data: 1
data: -1
data: 2
done
```

The source error is consumed by this operator. It is not also forwarded
downstream.

## Recovery Does Not Resubscribe

`recoverValue` keeps the existing source subscription:

```text
source error
  -> recover(error, stackTrace)
  -> one replacement data event
  -> wait for the next event from the same subscription
```

It does not restart the source, replay earlier data, or switch to another
stream. Those behaviors require different ownership and completion rules and
remain later steps.

## Callback Failure

If `recover` itself throws, that new exception becomes the downstream error
event:

```text
source error
  -> recover throws recovery error
  -> downstream recovery error
  -> later source events can still arrive
```

The original error was handled, so it is not emitted in addition to the
recovery error. The recovery error uses the stack trace captured where the
callback threw.

## Completion After Recovery

Recovery and completion are separate events. `recoverValue` does not close its
output after producing a fallback value. Every source error can be recovered
independently, and the existing source subscription remains active:

```text
source error 1 -> replacement data 1
source error 2 -> replacement data 2
source done    -> downstream done
```

Only source done closes the transformed stream, and it is forwarded exactly
once. A recovery callback failure is also just an error event; it does not
close the stream by itself.

## Preserved Behavior

Data and done events bypass the recovery callback and are forwarded normally.
The operator preserves whether the source is single-subscription or broadcast.
For a broadcast source, each downstream subscription runs its own recovery
callback for the error events it receives.
