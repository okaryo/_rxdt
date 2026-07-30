# Error Forwarding Through Operator Chains

Before implementing recovery, this project establishes the behavior of an
unhandled source error passing through an operator chain:

```text
source
  -> tap
  -> mapValue
  -> filterValue
  -> distinctValue
  -> listener
```

The source sends:

```text
data: 1
error: source failed
data: 1
data: 2
done
```

The chain maps values by multiplying them by ten and removes consecutive
duplicates. The listener receives:

```text
data: 10
error: source failed
data: 20
done
```

## Forwarding Is Not Handling

None of these operators has recovery behavior. They forward a source error by
passing the same error object and stack trace downstream:

```dart
outputSink.addError(error, stackTrace);
```

Forwarding preserves the event. Handling would make a new decision, such as
replacing the error with a data value, suppressing it, or switching to another
stream.

The test uses identity checks for both the error and stack trace. This verifies
that chaining multiple transformers does not accidentally create replacement
objects or lose the source stack.

## Errors Bypass Data Logic

The data callbacks in `tap`, `mapValue`, and `filterValue` are not invoked for
an error event. `distinctValue` also leaves its remembered previous value
unchanged.

That is why the second source value `1`, which arrives after the error, remains
a duplicate of the first successfully emitted value and is suppressed.

## Error And Done Are Separate

The source remains open after `addError`, so it can send later data. The error
does not imply completion:

```text
addError(error, stackTrace)
  -> error event
  -> subscription remains active

close()
  -> done event
  -> subscription completes
```

The operator chain forwards done exactly once only after the source is closed.
A later recovery operator can therefore handle one error while continuing to
observe the same source, without manufacturing or delaying completion.
