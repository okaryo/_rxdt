# Delaying Data With Timers

`delayValue` shifts each source data event into the future by creating a
`Timer` for it.

## Public API

```dart
extension RxdtDelayStreamExtensions<T> on Stream<T> {
  Stream<T> delayValue(Duration duration);
}
```

## Data Path

Each data event owns a separate timer:

```text
source data:1 -> Timer(duration) -> output data:1
source data:2 -> Timer(duration) -> output data:2
```

Creating the delayed stream is lazy. The source subscription begins only when
the delayed output is listened to, and timers are created only when source
data reaches that subscription.

## Completion Must Wait

Source done and output done are different moments when timers remain:

```text
source data:1 -> timer pending
source done   -> remember sourceDone
timer fires   -> output data:1
             -> no pending timers remain
             -> output done
```

Closing output as soon as source done arrives would discard every delayed data
event. The operator therefore closes only when both conditions are true:

```text
sourceDone && pendingTimers.isEmpty
```

Calling `output.add(value)` immediately before `output.close()` is safe because
the controller preserves the order of events added to it: downstream observes
the delayed data before done.

## Current Error And Lifecycle Boundaries

Like RxDart 0.28.0's `delay`, this first implementation delays data events but
forwards source errors immediately. This means a later source error can be
observed before an earlier data event whose timer is still pending.

Downstream cancellation cancels all owned timers and the source subscription.
Pausing pauses the source subscription, while already-running timers continue;
values they add are buffered by the paused output subscription. Focused tests
for these choices, including error, pause, and cancellation races, are the next
learning unit.

The output currently uses a single-subscription controller even for a
broadcast source. Preserving stream kind would require allocating timer state
and cleanup ownership independently for every downstream listener.

## Testing Limitation

The initial test uses a short real duration and checks event boundaries rather
than an exact elapsed-time threshold. A later step can introduce fake time so
timer behavior can be advanced deterministically without relying on wall-clock
speed.
