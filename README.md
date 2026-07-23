# _rxdt

`_rxdt` is a learning-oriented implementation of small reactive extensions for
Dart streams.

The goal of this project is not to build a production-ready replacement for
RxDart. The goal is to deeply understand Dart's `Stream` abstraction and the
mechanics hidden behind reactive APIs: event delivery, listening, subscription
lifecycle, transformation, error propagation, cancellation, pausing,
single-subscription and broadcast behavior, stateful operators, stream
composition, multicasting, and subjects.

## Purpose

This project is for learning Dart streams until their behavior can be
understood, explained, and implemented deliberately.

This repository follows the learning-project approach described in
`LEARNING_PROJECT.md`: small milestones, inspectable changes, and optional
advanced topics after the core goal is met.

The project assumes that the learner is already comfortable with basic Dart
syntax and ordinary application development. The focus is on the deeper
behavior of asynchronous event sequences and on the design choices behind
RxDart-style APIs.

## Learning Topics

This project may cover topics such as:

- Stream lifecycle: creation, listening, data events, error events, done events,
  and lazy execution.
- Event delivery: synchronous and asynchronous controllers, the event loop,
  microtasks, reentrancy, ordering, and zones.
- Stream kinds: single-subscription streams, broadcast streams, cold and hot
  event sources, and what changes when multiple listeners are involved.
- Subscriptions: pause, resume, cancel, callback replacement, and cleanup of
  upstream resources.
- Controllers and sinks: `StreamController`, `StreamSink`, ownership,
  backpressure-like behavior, and correct closing.
- Transformations: `StreamTransformer`, extension methods, event forwarding,
  and preservation of errors and completion.
- Operators: stateless and stateful filtering, mapping, accumulation,
  distinctness, buffering, and the state owned by each subscription.
- Composition: merging, concatenating, switching, combining latest values, and
  coordinating completion and cancellation across multiple sources.
- Error handling: forwarding, recovery, retry, fallback streams, stack traces,
  and the distinction between an error event and a thrown exception.
- Time-based behavior: delay, debounce, throttle, timeout, timers, and
  deterministic testing.
- Multicasting and subjects: sharing one source, replaying values, caching the
  latest value, listener lifecycle, and resource ownership.
- Robustness: tests for ordering, cancellation, reentrancy, leaks, and behavior
  comparisons with Dart's standard library and RxDart.

## Non-goals

The following are not the main focus of this project:

- Building a full production-ready replacement for RxDart.
- Reimplementing every RxDart operator or matching every edge case.
- Replacing Dart's core `Stream`, `StreamController`, or `StreamSubscription`
  abstractions.
- Designing a general-purpose concurrency runtime or scheduler framework.
- Prioritizing API breadth over understanding event and subscription behavior.

Production-oriented behavior may still be explored when it reveals an important
property of Dart streams or reactive programming.

## Core Milestone

The core learning milestone is not complete yet.

The first target is to make the smallest source-to-listener lifecycle visible by
building a tiny `tap`-style stream operator that:

- Is exposed as an extension on Dart's standard `Stream`.
- Does no work until the returned stream is listened to.
- Observes data events without changing them.
- Forwards data, error, and done events correctly.
- Makes source, transformer, subscription, and listener boundaries explicit.
- Includes tests or examples that show event ordering and lifecycle behavior.

Pausing, cancellation, broadcast behavior, stateful operators, stream
composition, timing, multicasting, and subjects should be introduced as later
learning steps.

## Approach

The preferred starting point is a deliberately tiny reactive extension:

1. Create a standard Dart stream with a small, observable event lifecycle.
2. Listen to it and record data, error, and done events.
3. Implement a `tap`-style extension without changing event values.
4. Express the transformation through an explicit `StreamTransformer`.
5. Verify laziness, event ordering, error forwarding, and completion.
6. Explore pause, resume, cancellation, and single-subscription versus
   broadcast behavior.
7. Add stateful operators, composition, time-based behavior, and subjects only
   after the basic event and subscription lifecycle is easy to explain.

The detailed learning-project operating pattern is documented in
`LEARNING_PROJECT.md`.

## Current State

The repository contains the initial learning plan and a minimal Dart library
package. The first stream operator has not been implemented yet.

## Project Documents

- `README.md`: project purpose, scope, and high-level learning direction.
- `AGENTS.md`: working instructions for AI agents and future contributors.
- `LEARNING_PROJECT.md`: reusable AI-assisted learning project pattern.
- `TODO.md`: living learning roadmap and progress tracker.
- `docs/project-layout.md`: initial public API and internal layout decisions.
