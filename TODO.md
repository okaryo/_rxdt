# TODO

This file is the living roadmap for the small RxDart-style implementation in
Dart. See `LEARNING_PROJECT.md` for the reusable learning-project pattern behind
this roadmap.

The roadmap is intentionally flexible. Update it whenever the learning goal,
implementation direction, or level of detail changes.

## Current Learning Goal

Build small reactive extensions on Dart's standard `Stream` and use them to
deeply understand, explain, and implement the mechanics usually hidden by
RxDart-style APIs.

Initial focus:

- The source-to-listener event lifecycle.
- Data, error, and done event delivery.
- Lazy stream execution and subscription ownership.
- A minimal extension method and `StreamTransformer`.
- Single-subscription and broadcast behavior.
- Pause, resume, cancellation, and cleanup.
- Later exploration of stateful operators, composition, timing, multicasting,
  and subjects.

## Core Milestone Status

Core milestone not complete.

The initial core milestone is to make one complete stream transformation
lifecycle visible: listen to a source, observe events in a tiny `tap`-style
operator, forward data, error, and done unchanged, and clean up correctly. More
complex reactive behavior should be introduced only after this lifecycle is
easy to inspect and explain.

## Roadmap

Roadmap sections are learning themes, not single work units.

### 0. Project Setup

- [x] Define the project purpose.
- [x] Create initial project documentation.
- [x] Copy the reusable learning-project pattern.
- [x] Decide the first implementation milestone.
- [x] Initialize the Dart package structure.
- [x] Decide the initial public API and internal file layout.
- [x] Decide how to organize learning notes and executable examples.

First implementation milestone:

- Build a minimal `tap`-style extension on `Stream<T>`.
- Observe a data event without changing its value.
- Forward data, error, and done events.
- Demonstrate that the source remains lazy until a listener subscribes.
- Add focused tests or an example that records the event order.
- Keep pausing, cancellation, broadcast behavior, stateful operators,
  composition, timing, and subjects out of scope until this basic lifecycle is
  visible.

### 1. Native Stream Lifecycle

- [x] Create a small stream using `Stream.fromIterable`, `async*`, or a
  `StreamController`.
- [x] Observe when stream production starts.
- [x] Record data, error, and done callbacks in order.
- [x] Compare an error event with an exception thrown outside the stream.
- [x] Observe the `Future` returned by `Stream#toList` or
  `StreamSubscription#asFuture`.
- [x] Document the first source-to-listener lifecycle.

Questions to answer:

- When does a Dart stream begin producing events?
- What is owned by the stream and what is owned by a subscription?
- How are data, error, and done represented?
- What happens when a listener callback throws?

### 2. First Transformer And Operator

- [x] Add an extension method on `Stream<T>`.
- [x] Implement a minimal `tap`-style operator.
- [x] Express the operator through an explicit `StreamTransformer`.
- [x] Forward data events without changing their values.
- [x] Forward error events with their stack traces.
- [x] Forward the done event exactly once.
- [ ] Verify that creating the transformed stream does not eagerly listen.
- [x] Add tests for data, error, done, and ordering.

Questions to answer:

- What does `StreamTransformer#bind` receive and return?
- When is the source stream actually listened to?
- Which callbacks must an identity-like operator preserve?
- What behavior is provided by `StreamTransformer.fromHandlers`, and what does
  it hide?

### 3. Subscription Lifecycle

- [ ] Observe `onListen`, `onPause`, `onResume`, and `onCancel`.
- [ ] Propagate pause and resume to the upstream subscription.
- [ ] Propagate cancellation and await asynchronous cleanup.
- [ ] Verify that no downstream events arrive after cancellation.
- [ ] Explore callback replacement on `StreamSubscription`.
- [ ] Add tests for cleanup and cancellation races.

Questions to answer:

- Does cancellation complete immediately?
- Who is responsible for cleaning up an upstream resource?
- What happens to buffered events while a subscription is paused?
- How should an operator behave when cleanup is asynchronous?

### 4. Event Delivery And Stream Kinds

- [ ] Compare asynchronous and synchronous `StreamController` delivery.
- [ ] Observe event-loop and microtask ordering.
- [ ] Explore reentrant event production and its restrictions.
- [ ] Compare single-subscription and broadcast streams.
- [ ] Observe the behavior of late and multiple listeners.
- [ ] Relate cold and hot terminology to concrete Dart stream behavior.
- [ ] Decide which stream kind each operator preserves.

Questions to answer:

- When can a data callback run relative to `listen`?
- Why can synchronous delivery create reentrancy hazards?
- What behavior changes when `asBroadcastStream` is used?
- Are "single-subscription versus broadcast" and "cold versus hot" the same
  distinction?

### 5. Stateless And Stateful Operators

- [ ] Implement one stateless transforming operator.
- [ ] Implement one stateless filtering operator.
- [ ] Implement `distinct` or another small stateful operator.
- [ ] Ensure state is owned per subscription.
- [ ] Decide how operator callbacks surface thrown exceptions.
- [ ] Compare selected behavior with Dart's built-in stream methods.
- [ ] Compare selected behavior with RxDart.

Questions to answer:

- Which operators need state?
- Where should operator state be allocated?
- How does a thrown mapper or predicate exception become a stream error?
- Which built-in `Stream` transformations already provide the desired
  semantics?

### 6. Error, Completion, And Recovery

- [ ] Preserve errors and stack traces across transformations.
- [ ] Implement a small error-recovery operator.
- [ ] Explore replacing an error with a value versus another stream.
- [ ] Explore retry only after resubscription semantics are clear.
- [ ] Verify completion behavior after recovery.
- [ ] Test synchronous exceptions and asynchronous error events separately.

Questions to answer:

- What is the difference between forwarding and handling an error?
- When can an operator continue after an error event?
- What does retry resubscribe to?
- Which stack trace should reach the downstream listener?

### 7. Combining Streams

- [ ] Implement concatenation for two streams.
- [ ] Implement merging for two streams.
- [ ] Coordinate errors, completion, pause, and cancellation across sources.
- [ ] Explore switch-to-latest behavior.
- [ ] Explore combine-latest behavior.
- [ ] Add deterministic tests for interleaved event sequences.

Questions to answer:

- When should a combined stream close?
- How are multiple upstream subscriptions owned and cancelled?
- Which ordering guarantees are possible when sources interleave?
- Why do switching and latest-value operators require state?

### 8. Time-Based Operators

- [ ] Implement delay using `Timer`.
- [ ] Explore debounce and throttle semantics.
- [ ] Define what happens to pending timers on pause or cancellation.
- [ ] Test ordering when data, errors, completion, and timers race.
- [ ] Introduce fake time or another deterministic test strategy if useful.
- [ ] Document timing limitations.

Questions to answer:

- Who owns each timer?
- Should completion wait for pending delayed events?
- How do debounce and throttle differ?
- How can time-based behavior be tested without flaky wall-clock waits?

### 9. Multicasting And Subjects

- [ ] Share one source subscription among multiple listeners.
- [ ] Define connect and disconnect behavior.
- [ ] Implement a minimal publish-style subject if useful.
- [ ] Explore latest-value or replay behavior.
- [ ] Define terminal behavior for late listeners.
- [ ] Handle listener-count transitions and upstream cleanup.
- [ ] Compare selected behavior with RxDart subjects and connectable streams.

Questions to answer:

- Who owns the shared upstream subscription?
- When should the shared source be connected or cancelled?
- Is a subject a stream, a sink, or both?
- Which values or terminal events should late listeners receive?

### 10. Robustness And Diagnostics

- [ ] Add clear project-specific error types where they improve understanding.
- [ ] Add event-sequence helpers for test diagnostics.
- [ ] Test reentrancy, duplicate completion, late events, and cancellation
  races.
- [ ] Check controllers, subscriptions, and timers for resource leaks.
- [ ] Add focused behavior comparisons with Dart's standard library.
- [ ] Add focused behavior comparisons with RxDart.
- [ ] Document known limitations.

Questions to answer:

- What makes a failed stream lifecycle test easy to diagnose?
- Which invariants should every operator preserve?
- Which RxDart behaviors are intentionally not copied?
- What limitations are acceptable for a learning implementation?

### 11. Optional Advanced Topics

- [ ] Explore custom `Stream` and `StreamSubscription` implementations.
- [ ] Explore zones and uncaught asynchronous errors in more depth.
- [ ] Explore isolates and streams crossing isolate boundaries.
- [ ] Explore backpressure patterns beyond subscription pause and resume.
- [ ] Explore windowing, buffering, grouping, and higher-order streams.
- [ ] Explore performance, allocation behavior, and operator fusion.
- [ ] Explore interoperability with Flutter lifecycle and state management.

These are optional directions, not required steps. They should be started only
when there is a specific learning question worth answering.

## Learning Log

Use this section to record notable decisions, discoveries, and direction
changes.

- Initial direction: focus on Dart `Stream` internals and RxDart-style reactive
  behavior rather than building a production-ready RxDart replacement.
- First implementation milestone: start with an identity-like `tap` operator so
  the complete event path can be observed before adding transformation state,
  multiple sources, timing, multicasting, or subjects.
- Use Dart's standard `dart:async` abstractions as the implementation foundation
  and add RxDart only when a focused behavior comparison is useful.
- Initialized a minimal Dart library package with `lib/rxdt.dart` as its empty
  public entry point. No operator implementation was added during package
  setup.
- Chose `package:rxdt/rxdt.dart` as the single public import and a
  `Stream<T>` extension as the first API. Operator implementations will live
  under `lib/src/operators/`, with behavior tested through the public import.
- Added an `async*` lifecycle experiment showing that creating the stream does
  not run the generator body and that production begins only after `listen`.
- Recorded data, error, and done callbacks from an `async*` source. An uncaught
  exception in the generator becomes an error event with a stack trace, then
  the terminated generator closes the stream.
- Compared error delivery paths: a synchronous exception is caught around its
  function call, while an error event from an `async*` source is delivered to
  the subscription's `onError` callback rather than thrown from `listen`.
- Observed `Stream#toList`: it starts a subscription immediately, completes
  with all collected values only after done, and completes with an error rather
  than a partial list when the stream fails.
- Added the first `tap` extension and a private transformer. Its data handler
  observes each event, then adds the same value to the downstream sink.
- Verified that `tap` forwards error objects and stack traces unchanged,
  preserves data/error/done ordering, and delivers done exactly once.
