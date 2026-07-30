import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test(
    'an operator chain preserves a source error, stack trace, and completion',
    () async {
      final expectedError = StateError('source failed');
      final expectedStackTrace = StackTrace.current;
      final tappedData = <int>[];
      final downstreamEvents = <String>[];
      final done = Completer<void>();
      final controller = StreamController<int>();
      Object? downstreamError;
      StackTrace? downstreamStackTrace;
      var doneCount = 0;

      controller.stream
          .tap(tappedData.add)
          .mapValue((value) => value * 10)
          .filterValue((value) => value.isEven)
          .distinctValue()
          .listen(
            (value) => downstreamEvents.add('data:$value'),
            onError: (Object error, StackTrace stackTrace) {
              downstreamError = error;
              downstreamStackTrace = stackTrace;
              downstreamEvents.add('error');
            },
            onDone: () {
              doneCount++;
              downstreamEvents.add('done');
              done.complete();
            },
          );

      controller.add(1);
      controller.addError(expectedError, expectedStackTrace);
      controller.add(1);
      controller.add(2);
      await controller.close();
      await done.future;

      expect(tappedData, [1, 1, 2]);
      expect(downstreamEvents, ['data:10', 'error', 'data:20', 'done']);
      expect(downstreamError, same(expectedError));
      expect(downstreamStackTrace, same(expectedStackTrace));
      expect(doneCount, 1);
    },
  );
}
