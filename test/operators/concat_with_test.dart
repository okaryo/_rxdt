import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test(
    'concatWith listens to the second stream only after first done',
    () async {
      final firstListened = Completer<void>();
      final secondListened = Completer<void>();
      final done = Completer<void>();
      final first = StreamController<int>(onListen: firstListened.complete);
      final second = StreamController<int>(onListen: secondListened.complete);
      final downstreamEvents = <String>[];

      final concatenated = first.stream.concatWith(second.stream);

      expect(first.hasListener, isFalse);
      expect(second.hasListener, isFalse);

      concatenated.listen(
        (value) => downstreamEvents.add('data:$value'),
        onError: (Object _) => downstreamEvents.add('error'),
        onDone: () {
          downstreamEvents.add('done');
          done.complete();
        },
      );

      await firstListened.future;

      expect(first.hasListener, isTrue);
      expect(second.hasListener, isFalse);

      first.add(1);
      first.add(2);
      await first.close();
      await secondListened.future;

      expect(downstreamEvents, ['data:1', 'data:2']);

      second.add(3);
      await second.close();
      await done.future;

      expect(downstreamEvents, ['data:1', 'data:2', 'data:3', 'done']);
    },
  );

  test(
    'concatWith forwards a first-stream error and waits for first done',
    () async {
      final expectedError = StateError('first failed');
      final expectedStackTrace = StackTrace.current;
      final secondListened = Completer<void>();
      final errorReceived = Completer<void>();
      final done = Completer<void>();
      final first = StreamController<int>();
      final second = StreamController<int>(onListen: secondListened.complete);
      final downstreamEvents = <String>[];
      Object? downstreamError;
      StackTrace? downstreamStackTrace;

      first.stream
          .concatWith(second.stream)
          .listen(
            (value) => downstreamEvents.add('data:$value'),
            onError: (Object error, StackTrace stackTrace) {
              downstreamError = error;
              downstreamStackTrace = stackTrace;
              downstreamEvents.add('error');
              errorReceived.complete();
            },
            onDone: () {
              downstreamEvents.add('done');
              done.complete();
            },
          );

      first.add(1);
      first.addError(expectedError, expectedStackTrace);
      await errorReceived.future;

      expect(downstreamEvents, ['data:1', 'error']);
      expect(second.hasListener, isFalse);

      await first.close();
      await secondListened.future;

      second.add(2);
      await second.close();
      await done.future;

      expect(downstreamEvents, ['data:1', 'error', 'data:2', 'done']);
      expect(downstreamError, same(expectedError));
      expect(downstreamStackTrace, same(expectedStackTrace));
    },
  );

  test('concatWith currently returns a single-subscription stream', () {
    final first = const Stream<int>.empty(broadcast: true);
    final second = const Stream<int>.empty(broadcast: true);

    final concatenated = first.concatWith(second);

    expect(concatenated.isBroadcast, isFalse);
  });
}
