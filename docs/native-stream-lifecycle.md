# Native Stream Lifecycle

The first experiment uses an asynchronous generator because its body gives us a
visible event-production boundary:

```dart
Stream<int> observedSource(List<String> events) async* {
  events.add('source:start');
  yield 1;
  events.add('source:after-yield');
}
```

Calling `observedSource` creates a `Stream<int>`, but it does not immediately
execute the generator body. Production starts only after `listen` creates a
subscription.

The tests deliberately observe the source and listener separately.

```text
source:start
source:after-yield
```

```text
listener:data:1
listener:done
```

This separation makes three guaranteed properties visible:

1. Creating the stream does not execute the asynchronous generator body.
2. Listening starts the generator.
3. Listener callbacks are not invoked before `listen` returns, and the listener
   receives data before done.

The `yield` adds a data event to the stream. The implementation may suspend the
generator around a yield, but the language does not require suspension after
every yielded value. Reaching the end of the generator body eventually closes
the stream and produces the done event.

The tests do not require `source:start` to occur after `listen` returns. The
current Dart implementation schedules the generator body asynchronously, but
the language-level contract only says that listening starts the body.
Likewise, the tests do not impose a total ordering between source-side effects
and listener callbacks.

## Data, Error, And Done

An uncaught exception inside an `async*` function is emitted as an error event
on its stream:

```dart
Stream<int> observedErrorSource(Object error) async* {
  yield 1;
  throw error;
}
```

With `cancelOnError: false`, the listener observes:

```text
listener:data:1
listener:error
listener:done
```

The thrown object reaches `onError` together with a stack trace. It does not
escape synchronously from the call to `listen`. In this example, the uncaught
exception terminates the generator body, so the error event is followed by the
done event.

An error event does not universally mean that every stream must close. A stream
source can emit an error and later emit more events. The generator closes here
because its own body terminates after the uncaught exception.

This experiment does not yet compare an error event with an exception thrown
outside a stream. It also does not cover cancellation, pause and resume, or
multiple listeners.
