import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test(
    'recoverValue handles an asynchronously delivered error event',
    () async {
      final expectedError = StateError('source failed');
      final expectedStackTrace = StackTrace.current;
      final downstreamEvents = <String>[];
      final replacementReceived = Completer<void>();
      final done = Completer<void>();
      final controller = StreamController<int>();
      Object? recoveredError;
      StackTrace? recoveredStackTrace;
      var recoveryCount = 0;

      controller.stream
          .recoverValue((error, stackTrace) {
            recoveryCount++;
            recoveredError = error;
            recoveredStackTrace = stackTrace;
            return -1;
          })
          .listen((value) {
            downstreamEvents.add('data:$value');
            replacementReceived.complete();
          }, onDone: done.complete);

      controller.addError(expectedError, expectedStackTrace);

      expect(recoveryCount, 0);
      expect(downstreamEvents, isEmpty);

      await replacementReceived.future;

      expect(recoveryCount, 1);
      expect(recoveredError, same(expectedError));
      expect(recoveredStackTrace, same(expectedStackTrace));
      expect(downstreamEvents, ['data:-1']);

      await controller.close();
      await done.future;
    },
  );

  test(
    'recoverValue cannot handle an exception thrown before a stream exists',
    () {
      final expectedError = StateError('source creation failed');
      Object? caughtByCaller;
      var recoveryCount = 0;

      Stream<int> createSource() => throw expectedError;

      try {
        createSource()
            .recoverValue((_, _) {
              recoveryCount++;
              return -1;
            })
            .listen(null);
      } catch (error) {
        caughtByCaller = error;
      }

      expect(caughtByCaller, same(expectedError));
      expect(recoveryCount, 0);
    },
  );
}
