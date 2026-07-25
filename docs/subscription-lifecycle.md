# Subscription Lifecycle

A `StreamSubscription` represents the active relationship between a stream
source and one listener. Operations on that subscription can notify the source
through `StreamController` lifecycle callbacks.

The first experiment records:

```text
listen()
  -> controller.onListen

subscription.pause()
  -> controller.onPause

subscription.resume()
  -> controller.onResume

subscription.cancel()
  -> controller.onCancel
```

The test uses one `Completer<void>` for each callback. This avoids depending on
whether a particular callback happens during the lifecycle method call or in a
later asynchronous turn. The test asserts only that each requested transition
is eventually observed in the expected order.

## Responsibilities

The subscription exposes controls to the consumer:

```dart
subscription.pause();
subscription.resume();
await subscription.cancel();
```

The controller callbacks expose those state changes to the producer:

```dart
StreamController<int>(
  onListen: startProducing,
  onPause: pauseProducing,
  onResume: resumeProducing,
  onCancel: stopProducing,
);
```

This is the coordination boundary between consumer demand and producer work.
The callbacks do not automatically pause a timer, close a file, or stop another
event source. They give the producer an opportunity to implement those actions.

This experiment does not yet inspect event buffering while paused, asynchronous
cancellation cleanup, or propagation through the `tap` transformer.
