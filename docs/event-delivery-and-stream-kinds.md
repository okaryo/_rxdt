# Event Delivery And Stream Kinds

## Asynchronous And Synchronous Controllers

`StreamController<T>()` creates an asynchronous controller by default.
Calling `add` accepts the event immediately, but an active listener receives it
only after the current synchronous execution has finished:

```text
before add
add event
after add
listener receives event
```

The test records that distinction directly:

```dart
events.add('before:add');
controller.add(1);
events.add('after:add');
```

Before waiting for delivery, the asynchronous result is:

```text
before:add
after:add
```

After delivery it becomes:

```text
before:add
after:add
listener:data:1
```

Setting `sync: true` changes where the callback can run:

```dart
final controller = StreamController<int>(sync: true);
```

For an active, unpaused subscription, the listener runs inside the call to
`add`:

```text
before add
add event
  -> listener receives event
add returns
after add
```

The synchronous result is therefore:

```text
before:add
listener:data:1
after:add
```

In both cases, `add` itself is called before the following line of producer
code. The difference is whether delivery occurs inside that call or after the
current synchronous work yields.

Synchronous delivery does not introduce another thread or parallel execution.
It allows listener code to reenter the producer's call stack. That can expose
partially updated state, make a slow listener delay `add`, or cause a listener
to invoke the producer again while an event is still being delivered.

An asynchronous controller avoids that reentrancy by default. A synchronous
controller is mainly useful for carefully controlled forwarding of an event
that already arrived asynchronously, when avoiding another asynchronous delay
is important.

## Event Loop And Microtask Ordering

One turn begins by running synchronous code to completion. Dart then drains the
microtask queue before taking the next event, such as a `Timer`, from the event
queue.

The experiment schedules work in this order:

```text
synchronous start
schedule microtask before add
add stream event
schedule microtask after add
schedule Timer event
synchronous end
```

The asynchronous controller schedules delivery of its pending event as a
microtask. Microtasks already in the queue run first, and microtasks added
during another microtask are placed after work already waiting in that queue.
The observed order is:

```text
sync:start
sync:end
microtask:before-add
stream:data:1
microtask:after-add
microtask:from-listener
event-queue:timer
```

This gives three useful boundaries:

1. No asynchronous callback interrupts the current synchronous execution.
2. The microtask queue is drained in scheduling order before the next event
   queue item runs.
3. A microtask scheduled by the stream listener does not jump ahead of a
   microtask that was already queued.

Long synchronous work delays both queues. Continually scheduling more
microtasks can likewise prevent the event queue from reaching a `Timer`; this
is often called event-queue starvation.

## Reentrant Event Production

Reentrancy occurs when a callback invoked by a producer calls back into that
producer before the original operation has returned.

With a synchronous broadcast controller, a listener runs inside `add`. If that
listener calls `add` on the same controller, the second call occurs while the
first event is still being delivered:

```text
producer add(1)
  -> listener receives 1
    -> listener calls add(2)
      -> rejected with StateError
  -> listener returns
producer add(1) returns
```

A synchronous broadcast controller must finish delivering one event to all
current listeners before another `add`, `addError`, or `close` begins. Rejecting
the nested operation prevents different listeners from observing events in
inconsistent orders.

With an asynchronous broadcast controller, the original producer call has
already returned before the listener runs:

```text
producer add(1)
producer add(1) returns

listener receives 1
  -> listener calls add(2)
  -> data 2 is queued

listener receives 2
```

The listener still causes further production, but it does not reenter an active
`add` call. The second event is delivered later, preserving the event order.

This is one reason `sync: true` is a behavioral contract rather than a general
performance switch. Producer methods reachable from listeners must be audited
for nested calls and partially updated state.

## Single-Subscription And Broadcast Streams

This distinction describes how many subscriptions one `Stream` instance
accepts over its lifetime.

A stream from the default controller is single-subscription:

```dart
final controller = StreamController<int>();

controller.stream.isBroadcast; // false
```

It accepts exactly one call to `listen`. Canceling that subscription does not
reset the stream or make room for another listener:

```text
first listen
  -> accepted

first subscription cancels

second listen
  -> StateError
```

The word "single" therefore means one subscription over the stream instance's
lifetime, not merely one active subscription at a time.

A broadcast controller creates a stream that accepts any number of
subscriptions:

```dart
final controller = StreamController<int>.broadcast();

controller.stream.isBroadcast; // true

final first = controller.stream.listen(firstListener);
final second = controller.stream.listen(secondListener);
```

Each `listen` returns a separate `StreamSubscription`. Canceling one
subscription does not cancel the others or close the controller.

## Late And Multiple Broadcast Listeners

A broadcast stream sends an event only to subscriptions that are active for
that event. It does not retain an event as history for future listeners.

The experiment changes the listener set between events:

```text
add 0 with no listeners
  -> dropped

first listener subscribes

add 1
  -> first receives 1

second listener subscribes

add 2
  -> first receives 2
  -> second receives 2

first listener cancels

add 3
  -> second receives 3
```

The resulting histories are:

```text
first:  [1, 2]
second: [2, 3]
```

The second listener is late, so it does not receive `0` or `1`. Canceling the
first subscription affects only that subscription; it does not stop the source
or the second subscription.

For an asynchronous broadcast controller, being active only when `add` is
called is not sufficient. A listener must still be subscribed when the queued
event is actually delivered. This is why canceling before delivery can suppress
an event that was added while the subscription existed.

## Cold And Hot Terminology

Cold and hot are informal descriptions of the producer lifecycle:

- A cold source generally starts a fresh production lifecycle for a consumer.
- A hot source has a lifecycle independent of a particular consumer, so a
  consumer observes only the portion available while it is attached.

Single-subscription and broadcast are a different, formal Dart API
classification:

- Single-subscription answers that one `Stream` instance accepts one
  subscription.
- Broadcast answers that one `Stream` instance accepts multiple subscriptions.

The two axes often appear together as cold single-subscription streams and hot
broadcast streams, but one does not imply the other.

The tests use the conventional examples directly. A cold factory creates a
fresh stream and producer lifecycle for each consumer:

```text
create cold stream for first consumer
  -> producer starts
  -> first receives 1, 2

create cold stream for second consumer
  -> another producer starts
  -> second receives 1, 2
```

The test verifies both complete sequences and a production start count of two:

```text
first:  [1, 2]
second: [1, 2]
producer starts: 2
```

```dart
Stream<int> createSequence() async* {
  yield 1;
  yield 2;
}

createSequence().listen(firstListener);
createSequence().listen(secondListener);
```

By contrast, the hot example converts one source to a shared stream with
`asBroadcastStream`. The first listener starts the one upstream subscription.
After it receives `1`, a second listener joins before the source emits `2`:

```text
shared producer starts once

emit 1
  -> first receives 1

second listener joins

emit 2
  -> first receives 2
  -> second receives 2
```

The result makes the shared timeline visible:

```text
first:  [1, 2]
second: [2]
producer starts: 1
```

Although the first listener triggers the initial connection, the second
listener does not receive a fresh producer or a fresh sequence. It joins the
one already-running production lifecycle midway. That shared-lifecycle behavior
is the important hot property in this experiment.

Instead of inferring behavior from cold or hot alone, ask concrete questions:

1. What starts and stops the producer?
2. Does each listener get a fresh producer or share one?
3. Are events retained while no listener exists?
4. What does a late listener receive?

These experiments do not yet cover per-listener buffering while one broadcast
subscription is paused.
