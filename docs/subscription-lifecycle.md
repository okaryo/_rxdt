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

## Propagation Through Tap

The `tap` operator creates a downstream subscription and an internal upstream
subscription:

```text
consumer
  -> downstream tap subscription
  -> upstream source subscription
  -> StreamController
```

Pausing the downstream subscription pauses the internal source subscription, so
the source controller receives `onPause`. Resuming downstream similarly causes
the source controller to receive `onResume`.

```text
downstream.pause()
  -> upstream.pause()
  -> source onPause

downstream.resume()
  -> upstream.resume()
  -> source onResume
```

The current `tap` implementation gets this behavior from the subscription
plumbing provided by `StreamTransformer.fromHandlers`. The operator does not
need to call `pause` or `resume` manually.

## Cancellation And Asynchronous Cleanup

Canceling the downstream `tap` subscription cancels the internal upstream
subscription. The source controller observes that transition through
`onCancel`.

An `onCancel` callback may return a `Future<void>` while the producer releases a
resource. The future returned by the downstream subscription's `cancel()` does
not complete until that upstream cleanup finishes.

```text
downstream.cancel()
  -> upstream.cancel()
  -> source onCancel starts
  -> asynchronous cleanup
  -> source onCancel completes
  -> downstream cancel Future completes
```

Awaiting `cancel()` is therefore important when later work depends on the
resource having been released, not merely on event delivery having stopped.

## Event Delivery After Cancellation

Once `cancel()` completes, the subscription no longer delivers events. The
test first waits until one data event has passed through `tap`, then cancels the
downstream subscription. A later value added by the source reaches neither the
tap callback nor the downstream listener.

```text
source data: 1
  -> tap callback: 1
  -> downstream: 1

downstream.cancel()

source attempts data: 2
  -> no tap callback
  -> no downstream callback
```

Cancellation differs from a done event. Canceling stops this subscription
without invoking its `onDone` callback. For a broadcast stream, other active
subscriptions can still receive later events.

This experiment does not yet inspect event buffering while paused, callback
replacement, or races between queued events and cancellation.
