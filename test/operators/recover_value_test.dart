import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test(
    'recoverValue replaces a source error with data and continues',
    () async {
      final expectedError = StateError('source failed');
      final expectedStackTrace = StackTrace.current;
      final recoveredErrors = <Object>[];
      final recoveredStackTraces = <StackTrace>[];
      final downstreamEvents = <String>[];
      final done = Completer<void>();
      final controller = StreamController<int>();

      controller.stream
          .recoverValue((error, stackTrace) {
            recoveredErrors.add(error);
            recoveredStackTraces.add(stackTrace);
            return -1;
          })
          .listen(
            (value) => downstreamEvents.add('data:$value'),
            onError: (Object _) => downstreamEvents.add('error'),
            onDone: () {
              downstreamEvents.add('done');
              done.complete();
            },
          );

      controller.add(1);
      controller.addError(expectedError, expectedStackTrace);
      controller.add(2);
      await controller.close();
      await done.future;

      expect(recoveredErrors, [same(expectedError)]);
      expect(recoveredStackTraces, [same(expectedStackTrace)]);
      expect(downstreamEvents, ['data:1', 'data:-1', 'data:2', 'done']);
    },
  );

  test(
    'recoverValue turns a recovery callback failure into an error and continues',
    () async {
      final recoveryError = StateError('recovery failed');
      final downstreamEvents = <String>[];
      final done = Completer<void>();
      final controller = StreamController<int>();
      Object? downstreamError;
      StackTrace? downstreamStackTrace;

      controller.stream
          .recoverValue((_, _) => throw recoveryError)
          .listen(
            (value) => downstreamEvents.add('data:$value'),
            onError: (Object error, StackTrace stackTrace) {
              downstreamError = error;
              downstreamStackTrace = stackTrace;
              downstreamEvents.add('error');
            },
            onDone: () {
              downstreamEvents.add('done');
              done.complete();
            },
          );

      controller.addError(StateError('source failed'), StackTrace.current);
      controller.add(2);
      await controller.close();
      await done.future;

      expect(downstreamEvents, ['error', 'data:2', 'done']);
      expect(downstreamError, same(recoveryError));
      expect(downstreamStackTrace, isNotNull);
    },
  );

  test(
    'recoverValue stays open across recoveries and forwards source done once',
    () async {
      final downstreamEvents = <String>[];
      final recoveredTwice = Completer<void>();
      final done = Completer<void>();
      final controller = StreamController<int>();
      var recoveryCount = 0;
      var doneCount = 0;

      controller.stream
          .recoverValue((_, _) {
            recoveryCount++;
            return -recoveryCount;
          })
          .listen(
            (value) {
              downstreamEvents.add('data:$value');

              if (value == -2) {
                recoveredTwice.complete();
              }
            },
            onError: (Object _) => downstreamEvents.add('error'),
            onDone: () {
              doneCount++;
              downstreamEvents.add('done');
              done.complete();
            },
          );

      controller.addError(StateError('first'), StackTrace.current);
      controller.addError(StateError('second'), StackTrace.current);
      await recoveredTwice.future;

      expect(recoveryCount, 2);
      expect(downstreamEvents, ['data:-1', 'data:-2']);
      expect(done.isCompleted, isFalse);

      await controller.close();
      await done.future;

      expect(downstreamEvents, ['data:-1', 'data:-2', 'done']);
      expect(doneCount, 1);
    },
  );

  test('recoverValue preserves the source stream kind', () {
    final single = Stream.fromIterable([1]).recoverValue((_, _) => -1);
    final broadcast = const Stream<int>.empty(
      broadcast: true,
    ).recoverValue((_, _) => -1);

    expect(single.isBroadcast, isFalse);
    expect(broadcast.isBroadcast, isTrue);
  });
}
