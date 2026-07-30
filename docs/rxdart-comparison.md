# RxDart Comparison

RxDart is a development-only dependency in this project. The learning
implementation continues to depend only on `dart:async`; RxDart is present so
focused executable comparisons can document where the contracts match or
differ.

## Corresponding Operator

The closest RxDart operator to `tap` is `doOnData`:

```dart
source.tap(onData);
source.doOnData(onData);
```

Both call a side-effect callback for each ordinary data event and otherwise
preserve that value. Given `1, 2, 3`, both callbacks observe `1, 2, 3`, and both
downstream listeners receive `1, 2, 3`.

RxDart's `DoExtensions` covers more lifecycle points than the current learning
API, including data, error, done, listen, pause, resume, and cancellation.
`rxdt` currently exposes only the deliberately small data-only `tap`.

## Callback Failure Difference

The operators make different choices when their data callback throws.

`rxdt` replaces the triggering data event:

```text
source:     data 1, data 2, data 3, done
callback:          throws
tap output: data 1, error,  data 3, done
```

RxDart reports the callback failure and still forwards the observed data:

```text
source:          data 1, data 2, data 3, done
callback:                throws
doOnData output: data 1, error,  data 2, data 3, done
```

Both error events are non-terminal, so later source events still arrive.

This difference follows two reasonable interpretations:

- `tap`: callback execution is part of handling that data event, so failure
  replaces the event.
- `doOnData`: the callback only observes the event, so its failure adds an
  error but does not consume or replace the original data.

The current `tap` contract remains unchanged. The difference is recorded
explicitly rather than treating RxDart compatibility as an automatic
requirement.

## Other Current Operators

RxDart streams are still Dart `Stream` instances. For the basic behaviors in
this project, callers use the standard methods:

```dart
stream.map(convert);
stream.where(test);
stream.distinct(equals);
```

Therefore `mapValue`, `filterValue`, and `distinctValue` were already compared
with their relevant Dart standard-library counterparts. RxDart adds many
higher-level operators, but those should be compared only when a corresponding
learning implementation is introduced.
