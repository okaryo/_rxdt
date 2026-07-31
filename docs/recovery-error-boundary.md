# Synchronous Exceptions And Stream Error Events

`recoverValue` handles error events from an existing stream. It cannot handle
an exception thrown before that stream exists.

## Asynchronous Error Event

A default `StreamController` queues an error for later delivery:

```dart
controller.addError(error, stackTrace);
```

Immediately after `addError` returns, the recovery callback has not run yet.
When the subscription later receives the error event, `recoverValue` invokes
the callback and replaces the event with data:

```text
controller.addError returns
  -> asynchronous event delivery
  -> recoverValue handleError
  -> recovery callback
  -> replacement data
```

The error object and stack trace cross the asynchronous boundary without losing
their identities.

## Synchronous Source-Creation Exception

Consider a factory that throws instead of returning a stream:

```dart
Stream<int> createSource() {
  throw StateError('source creation failed');
}

createSource().recoverValue(recover);
```

Evaluation stops inside `createSource()`. There is no `Stream`, transformed
stream, or subscription yet, so `recoverValue` cannot receive anything. The
caller must catch this exception around the factory invocation.

```text
call createSource
  -> synchronous throw
  -> caller catch

recoverValue is never reached
```

## A `throw` Can Enter Either Path

The `throw` keyword alone does not determine whether something is a synchronous
exception or a stream error. Its execution context matters:

- throwing while synchronously creating the source escapes to the caller;
- throwing later inside an `async*` producer is converted into a stream error;
- throwing inside an operator callback can be caught by that operator and
  converted with `sink.addError`.

Recovery operators operate on the stream protocol—data, error, and done—not on
arbitrary exceptions that occur before or outside that protocol.
