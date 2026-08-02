# Multi-Source Subscription Lifecycle

Combining streams means the output subscription owns one or more upstream
subscriptions. Pause, cancellation, cleanup, errors, and done must be
coordinated according to which sources are currently active.

## Sequential Ownership In `concatWith`

`concatWith` owns only one active upstream subscription:

```text
first active
  -> first done
  -> second active
  -> second done
  -> output done
```

Canceling while first is active cancels first and waits for its asynchronous
cleanup. Cancellation terminates the asynchronous generator, so it never
advances to the second `yield*` and never subscribes to second.

## Concurrent Ownership In `mergeWith`

`mergeWith` owns both source subscriptions concurrently:

```text
output subscription
  -> first subscription
  -> second subscription
```

Pausing output pauses both sources. Events added while paused remain upstream
until the subscriptions resume; neither source may continue delivering into
the output while downstream has requested a pause.

Resuming output resumes both. There is no cross-source ordering guarantee for
events buffered independently during the pause.

## Cancellation And Cleanup

Canceling merged output requests cancellation from both sources immediately:

```text
downstream cancel
  -> first.cancel()  -> first cleanup future
  -> second.cancel() -> second cleanup future
  -> wait for both
  -> downstream cancel future completes
```

Completing only one cleanup is insufficient. `mergeWith` uses `Future.wait` so
the downstream cancellation future represents release of every resource it
owns. Events added after cancellation reach neither output nor its former
listener.

## Errors And Done

An error changes neither source ownership nor the completed-source count. With
the default `cancelOnError: false`, it is forwarded and both subscriptions stay
active.

Done does change ownership state:

- `concatWith` treats first done as the signal to replace its active
  subscription with second.
- `mergeWith` records each source done and closes output only after both have
  completed.

If downstream uses `cancelOnError: true`, delivery of an error causes
downstream cancellation, which then follows the same upstream cancellation and
cleanup rules.
