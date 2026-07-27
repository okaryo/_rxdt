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

These experiments do not yet exercise reentrant production restrictions or
compare single-subscription and broadcast streams.
