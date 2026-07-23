# Initial Public API And Project Layout

The first implementation should add reactive behavior to Dart's standard
`Stream` rather than introduce a replacement stream type.

## Public API

Users will import one library:

```dart
import 'package:rxdt/rxdt.dart';
```

The first operator will be an extension on `Stream<T>` with an API shaped like:

```dart
extension RxdtStreamExtensions<T> on Stream<T> {
  Stream<T> tap(void Function(T event) onData);
}
```

This keeps the standard `Stream<T>` type at both sides of the operator. Existing
Dart APIs can therefore produce the source stream and consume the transformed
stream without adapters.

The initial callback observes only data events. Error and done events must still
pass through the operator unchanged, but callbacks for observing those events
can be considered later when there is a specific learning reason to expand the
API.

## Internal Layout

Files will be introduced only when their implementation step begins. The
intended initial layout is:

```text
lib/
  rxdt.dart
  src/
    operators/
      tap.dart
test/
  operators/
    tap_test.dart
docs/
  project-layout.md
```

- `lib/rxdt.dart` is the package's public entry point. It explicitly exports
  public declarations.
- `lib/src/operators/tap.dart` will contain the public stream extension and its
  private transformer implementation.
- `test/operators/tap_test.dart` will exercise behavior through the public
  package import.
- `docs/` records the stream behavior learned during each implementation unit.

The transformer should remain private initially. It is an implementation
boundary used to study event forwarding, while the extension method is the API
that callers need.

## Deferred Decisions

The following decisions are intentionally deferred:

- A wrapper type around `Stream`.
- A common base class for transformers.
- Separate public libraries for groups of operators.
- RxDart-compatible names for every future operator.
- Subject, connectable stream, or scheduler namespaces.

Introducing those abstractions before a second use case exists would hide the
first operator's source-to-listener lifecycle without answering a current
learning question.
