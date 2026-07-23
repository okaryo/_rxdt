# AGENTS.md

This repository is a learning project for implementing small RxDart-style
reactive extensions on top of Dart streams. Agents should optimize for
understanding, incremental progress, and clear explanations rather than feature
volume.

## Project Intent

The project explores how Dart's `Stream`, `StreamController`,
`StreamSubscription`, and `StreamTransformer` work underneath RxDart-style
operators, stream composition, multicasting, and subjects.

The learner is already comfortable with basic Dart syntax and ordinary
application development. Avoid spending too much time on syntax or generic
project structure. Prefer deeper discussion of event delivery, subscription
lifecycle, synchronous versus asynchronous behavior, error and completion
semantics, cancellation, stream kinds, operator state, and resource ownership.

## Working Style

- Proceed step by step.
- Treat a request to proceed to the next step as permission to advance one small
  learning unit, not to complete an entire roadmap section, unless the user
  explicitly asks for a full section.
- Before a major implementation step, clarify the specific learning objective.
- After a meaningful implementation step, summarize what was learned and what
  remains unclear.
- Keep changes small and inspectable.
- Prefer Dart's standard `dart:async` APIs when the goal is to understand the
  mechanism.
- Do not add RxDart as an implementation dependency unless a specific learning
  step calls for a behavior comparison.
- Use external dependencies only when they directly support a learning
  question.
- Keep `TODO.md` updated as a living roadmap, not a fixed plan.
- If the learning direction changes, update the roadmap instead of forcing the
  original plan.

## Implementation Guidance

- Build extensions on Dart's standard `Stream` rather than replacing the core
  stream abstraction.
- Begin with an identity-like `tap` operator so the event path can be studied
  without also changing event values.
- Make source stream, transformer, returned stream, subscription, downstream
  listener, and cleanup boundaries explicit when each boundary has a learning
  reason to exist.
- Preserve data, error, and done behavior deliberately. Do not focus only on the
  happy-path data callback.
- Test laziness, ordering, pause, resume, cancellation, reentrancy,
  single-subscription behavior, and broadcast behavior as those topics enter
  scope.
- Treat per-listener state carefully. A stateful operator should not
  accidentally share mutable state across independent subscriptions.
- Be explicit about who owns each `StreamController`, timer, upstream
  subscription, and cleanup action.
- When introducing abstractions such as operator transformers, forwarding
  subscriptions, stream combinators, connectable streams, or subjects, explain
  what problem the abstraction solves and which lifecycle behavior it hides.
- When comparing with RxDart, focus on underlying behavior and tradeoffs rather
  than copying its full public API.
- Include focused tests and small reproducible examples for important behavior,
  especially event order, errors, completion, cancellation, multiple listeners,
  and timing.

## Documentation Guidance

- `README.md` should describe the project purpose and scope.
- `TODO.md` should track the current learning roadmap, progress, and open
  questions.
- Add notes to `TODO.md` when a completed step changes the next learning
  direction.
- Add topic-specific notes under `docs/` once an implementation step creates a
  useful learning artifact.
- Add executable demonstrations under `examples/` when observing an event
  sequence is more useful than prose alone.
- Use `LEARNING_PROJECT.md` only as background for the reusable learning-project
  pattern; keep this file focused on `_rxdt` execution guidance.
