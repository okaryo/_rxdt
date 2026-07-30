import 'dart:async';

import 'package:rxdart/rxdart.dart';
import 'package:test/test.dart';

void main() {
  test(
    'a recovery stream runs beside the still-active source subscription',
    () async {
      final expectedError = StateError('source failed');
      final expectedStackTrace = StackTrace.current;
      final recoveryListened = Completer<void>();
      final receivedOne = Completer<void>();
      final receivedTwo = Completer<void>();
      final receivedFallback = Completer<void>();
      final downstreamDone = Completer<void>();
      final source = StreamController<int>();
      final recovery = StreamController<int>(
        onListen: recoveryListened.complete,
      );
      final downstreamEvents = <String>[];
      Object? recoveryError;
      StackTrace? recoveryStackTrace;

      source.stream
          .onErrorResume((error, stackTrace) {
            recoveryError = error;
            recoveryStackTrace = stackTrace;
            return recovery.stream;
          })
          .listen(
            (value) {
              downstreamEvents.add('data:$value');

              switch (value) {
                case 1:
                  receivedOne.complete();
                case 2:
                  receivedTwo.complete();
                case -1:
                  receivedFallback.complete();
              }
            },
            onError: (Object _) => downstreamEvents.add('error'),
            onDone: () {
              downstreamEvents.add('done');
              downstreamDone.complete();
            },
          );

      source.add(1);
      await receivedOne.future;

      source.addError(expectedError, expectedStackTrace);
      await recoveryListened.future;

      source.add(2);
      await receivedTwo.future;
      await source.close();

      expect(recoveryError, same(expectedError));
      expect(recoveryStackTrace, same(expectedStackTrace));
      expect(downstreamEvents, ['data:1', 'data:2']);
      expect(downstreamDone.isCompleted, isFalse);

      recovery.add(-1);
      await receivedFallback.future;
      await recovery.close();
      await downstreamDone.future;

      expect(downstreamEvents, ['data:1', 'data:2', 'data:-1', 'done']);
    },
  );
}
