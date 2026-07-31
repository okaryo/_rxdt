import 'dart:async';

import 'package:rxdart/rxdart.dart';
import 'package:test/test.dart';

void main() {
  test(
    'Rx.retry creates and subscribes to a fresh source per attempt',
    () async {
      var attemptCount = 0;

      Stream<int> createAttempt() async* {
        attemptCount++;
        final currentAttempt = attemptCount;

        yield 1;

        if (currentAttempt < 3) {
          throw StateError('attempt $currentAttempt failed');
        }

        yield 2;
      }

      final retried = Rx.retry(createAttempt, 2);

      expect(attemptCount, 0);
      expect(await retried.toList(), [1, 1, 1, 2]);
      expect(attemptCount, 3);
    },
  );

  test(
    'Rx.retry emits every collected error after attempts are exhausted',
    () async {
      final expectedErrors = <StateError>[];
      final expectedStackTraces = <StackTrace>[];
      final downstreamErrors = <Object>[];
      final downstreamStackTraces = <StackTrace>[];
      final downstreamEvents = <String>[];
      final done = Completer<void>();
      var attemptCount = 0;

      Stream<int> createFailingAttempt() {
        attemptCount++;
        final error = StateError('attempt $attemptCount failed');
        final stackTrace = StackTrace.fromString('attempt $attemptCount');
        expectedErrors.add(error);
        expectedStackTraces.add(stackTrace);
        return Stream.error(error, stackTrace);
      }

      Rx.retry(createFailingAttempt, 1).listen(
        (value) => downstreamEvents.add('data:$value'),
        onError: (Object error, StackTrace stackTrace) {
          downstreamErrors.add(error);
          downstreamStackTraces.add(stackTrace);
          downstreamEvents.add('error');
        },
        onDone: () {
          downstreamEvents.add('done');
          done.complete();
        },
      );

      await done.future;

      expect(attemptCount, 2);
      expect(downstreamErrors, orderedEquals(expectedErrors));
      expect(downstreamStackTraces, orderedEquals(expectedStackTraces));
      expect(downstreamEvents, ['error', 'error', 'done']);
    },
  );
}
