import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test(
    'mergeWith listens to both sources and forwards arrival order',
    () async {
      final firstListened = Completer<void>();
      final secondListened = Completer<void>();
      final receivedOne = Completer<void>();
      final receivedTen = Completer<void>();
      final receivedTwo = Completer<void>();
      final receivedTwenty = Completer<void>();
      final done = Completer<void>();
      final first = StreamController<int>(onListen: firstListened.complete);
      final second = StreamController<int>(onListen: secondListened.complete);
      final downstreamEvents = <String>[];

      final merged = first.stream.mergeWith(second.stream);

      expect(first.hasListener, isFalse);
      expect(second.hasListener, isFalse);

      merged.listen(
        (value) {
          downstreamEvents.add('data:$value');

          switch (value) {
            case 1:
              receivedOne.complete();
            case 10:
              receivedTen.complete();
            case 2:
              receivedTwo.complete();
            case 20:
              receivedTwenty.complete();
          }
        },
        onDone: () {
          downstreamEvents.add('done');
          done.complete();
        },
      );

      await Future.wait([firstListened.future, secondListened.future]);

      expect(first.hasListener, isTrue);
      expect(second.hasListener, isTrue);

      first.add(1);
      await receivedOne.future;
      second.add(10);
      await receivedTen.future;
      first.add(2);
      await receivedTwo.future;

      await first.close();

      expect(done.isCompleted, isFalse);

      second.add(20);
      await receivedTwenty.future;
      await second.close();
      await done.future;

      expect(downstreamEvents, [
        'data:1',
        'data:10',
        'data:2',
        'data:20',
        'done',
      ]);
    },
  );

  test(
    'mergeWith forwards an error while the other source continues',
    () async {
      final expectedError = StateError('first failed');
      final expectedStackTrace = StackTrace.current;
      final errorReceived = Completer<void>();
      final secondDataReceived = Completer<void>();
      final done = Completer<void>();
      final first = StreamController<int>();
      final second = StreamController<int>();
      final downstreamEvents = <String>[];
      Object? downstreamError;
      StackTrace? downstreamStackTrace;

      first.stream
          .mergeWith(second.stream)
          .listen(
            (value) {
              downstreamEvents.add('data:$value');
              secondDataReceived.complete();
            },
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

      first.addError(expectedError, expectedStackTrace);
      await errorReceived.future;

      second.add(2);
      await secondDataReceived.future;

      await first.close();
      await second.close();
      await done.future;

      expect(downstreamEvents, ['error', 'data:2', 'done']);
      expect(downstreamError, same(expectedError));
      expect(downstreamStackTrace, same(expectedStackTrace));
    },
  );

  test('mergeWith currently returns a single-subscription stream', () {
    final first = const Stream<int>.empty(broadcast: true);
    final second = const Stream<int>.empty(broadcast: true);

    final merged = first.mergeWith(second);

    expect(merged.isBroadcast, isFalse);
  });
}
