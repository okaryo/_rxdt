# Completion After Recovery

Recovery changes what happens to an error, but it must also define what event
eventually closes the downstream stream.

| Recovery form | Downstream completion condition |
| --- | --- |
| `recoverValue` | The existing source sends done |
| RxDart `onErrorResume` | The source and every active recovery stream send done |
| RxDart `retry` succeeds | The successful attempt sends done |
| RxDart `retry` is exhausted | Collected errors are emitted, then done |

## Value Recovery

`recoverValue` creates no new subscription. Replacing an error with data does
not imply completion:

```text
error -> replacement data -> still open
done  -> downstream done
```

The focused test recovers two errors and verifies that downstream remains open
after both replacements. Closing the source then produces one done event.

## Stream Recovery

A recovery stream introduces another completion dependency. The source can be
done while a recovery subscription is still active, so downstream done must be
delayed until that recovery also completes.

## Retry

Each failed retry attempt is canceled and replaced by a new subscription. A
failed attempt's done is not the successful terminal event. Downstream closes
when one attempt completes successfully, or after the retry limit is reached
and the implementation has delivered its final errors.

Completion therefore follows resource ownership: an operator can close only
when every subscription whose future events it promises to forward has reached
its terminal state.
