# Retry And Resubscription

Retry does not convert an error into a value and does not continue the failed
subscription. It cancels the failed attempt and creates a new source
subscription.

## A Source Factory

RxDart's `Rx.retry` accepts a source factory:

```dart
Rx.retry(
  () => createSourceStream(),
  retryCount,
);
```

The factory is lazy. It is not called until the retry stream receives a
listener. It is then called once for the initial attempt and again for each
retry.

With `retryCount == 2`, at most three attempts occur:

```text
initial attempt
  -> first retry
  -> second retry
```

Using a factory matters because a failed single-subscription stream generally
cannot simply be listened to again. The factory can recreate the stream and any
per-attempt resources it owns.

## Restarting Means Repeating Earlier Data

The focused source emits `1` on every attempt. Its first two attempts then
fail, while its third emits `2` and completes:

```text
attempt 1: data 1, error
attempt 2: data 1, error
attempt 3: data 1, data 2, done
```

The retry output is:

```text
data 1, data 1, data 1, data 2, done
```

Data from failed attempts is not rolled back. A downstream consumer can
therefore observe duplicates or repeated side effects after retry. Operations
that must be performed exactly once need an idempotency strategy outside the
stream operator.

## Successful And Exhausted Retry

When a later attempt completes successfully, RxDart suppresses the errors from
the earlier failed attempts.

When the retry limit is exhausted, RxDart 0.28.0 emits every error it collected,
with each original stack trace, and then emits done:

```text
attempt 1 error
attempt 2 error
retry limit reached
  -> downstream error 1
  -> downstream error 2
  -> downstream done
```

This differs from an implementation that would emit only the final error.

## Ownership Boundary

Each attempt has a distinct source subscription:

```text
downstream retry subscription
  -> attempt 1 source subscription
     -> error, cancel
  -> attempt 2 source subscription
     -> error, cancel
  -> attempt 3 source subscription
     -> done
```

Retry must propagate downstream pause and resume to the current attempt,
cancel the current attempt when downstream cancels, and discard resources from
failed attempts before starting new ones.

This project records the behavior before implementing its own retry operator.
Questions such as delays, conditional retry, resource recreation, and unlimited
attempts should be decided explicitly rather than hidden behind a small API.
