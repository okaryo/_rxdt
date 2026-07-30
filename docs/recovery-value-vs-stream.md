# Recovering With A Value Versus A Stream

An error can be replaced with one value or with another stream, but those
choices have different subscription and completion behavior.

## One Replacement Value

`recoverValue` invokes a synchronous callback and adds its result directly to
the existing event path:

```text
source error
  -> recover(error, stackTrace)
  -> one replacement data event
  -> continue the same source subscription
```

It introduces no additional stream subscription. The source alone determines
completion.

## A Recovery Stream

RxDart's `onErrorResume` returns a stream from its recovery callback:

```text
source error
  -> recovery(error, stackTrace)
  -> listen to recovery stream
  -> forward zero or more recovery events
```

The focused test uses manually controlled source and recovery controllers. It
observes three important properties of RxDart 0.28.0:

1. The source subscription remains active after the error, so later source data
   still reaches downstream.
2. The recovery stream has its own subscription and can outlive the source.
3. Downstream done waits until the source and all active recovery streams have
   completed.

The observed sequence is:

```text
source data: 1
source error -> recovery stream is listened to
source data: 2
source done
recovery data: -1
recovery done
downstream done
```

Despite the common phrase "switch to a recovery stream," this behavior does
not permanently abandon the source. Source and recovery subscriptions can both
remain active, so their events may interleave.

## Additional Ownership

A value replacement only computes and forwards one value. A stream replacement
must additionally define:

- who owns each recovery subscription;
- whether source delivery is paused while recovery runs;
- how source and recovery events are ordered;
- how downstream pause and resume affect both subscriptions;
- what happens when downstream cancels;
- whether recovery errors are forwarded or recovered again;
- when downstream done is allowed.

These are the same coordination concerns that appear when combining multiple
streams. For that reason, this project does not add a custom stream-recovery
operator yet. Its contract and subscription plumbing should be revisited after
the combining-stream fundamentals are implemented.
