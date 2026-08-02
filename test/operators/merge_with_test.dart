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

  test('mergeWith pauses and resumes both source subscriptions', () async {
    final firstPaused = Completer<void>();
    final secondPaused = Completer<void>();
    final firstResumed = Completer<void>();
    final secondResumed = Completer<void>();
    final receivedBoth = Completer<void>();
    final done = Completer<void>();
    final downstreamData = <int>[];
    final first = StreamController<int>(
      onPause: firstPaused.complete,
      onResume: firstResumed.complete,
    );
    final second = StreamController<int>(
      onPause: secondPaused.complete,
      onResume: secondResumed.complete,
    );
    final subscription = first.stream.mergeWith(second.stream).listen((value) {
      downstreamData.add(value);

      if (downstreamData.length == 2) {
        receivedBoth.complete();
      }
    }, onDone: done.complete);

    subscription.pause();
    await Future.wait([firstPaused.future, secondPaused.future]);

    expect(first.isPaused, isTrue);
    expect(second.isPaused, isTrue);

    first.add(1);
    second.add(2);
    await Future<void>.delayed(Duration.zero);

    expect(downstreamData, isEmpty);

    subscription.resume();
    await Future.wait([firstResumed.future, secondResumed.future]);
    await receivedBoth.future;

    expect(first.isPaused, isFalse);
    expect(second.isPaused, isFalse);
    expect(downstreamData, unorderedEquals([1, 2]));

    await first.close();
    await second.close();
    await done.future;
  });

  test('mergeWith cancellation waits for both source cleanups', () async {
    final firstListened = Completer<void>();
    final secondListened = Completer<void>();
    final firstCancelStarted = Completer<void>();
    final secondCancelStarted = Completer<void>();
    final allowFirstCleanup = Completer<void>();
    final allowSecondCleanup = Completer<void>();
    final firstCleanupFinished = Completer<void>();
    final downstreamData = <int>[];
    final first = StreamController<int>(
      onListen: firstListened.complete,
      onCancel: () async {
        firstCancelStarted.complete();
        await allowFirstCleanup.future;
        firstCleanupFinished.complete();
      },
    );
    final second = StreamController<int>(
      onListen: secondListened.complete,
      onCancel: () async {
        secondCancelStarted.complete();
        await allowSecondCleanup.future;
      },
    );
    final subscription = first.stream
        .mergeWith(second.stream)
        .listen(downstreamData.add);
    var downstreamCancelCompleted = false;

    await Future.wait([firstListened.future, secondListened.future]);

    final downstreamCancel = subscription.cancel().then((_) {
      downstreamCancelCompleted = true;
    });

    await Future.wait([firstCancelStarted.future, secondCancelStarted.future]);

    expect(downstreamCancelCompleted, isFalse);

    allowFirstCleanup.complete();
    await firstCleanupFinished.future;

    expect(downstreamCancelCompleted, isFalse);

    allowSecondCleanup.complete();
    await downstreamCancel;

    expect(downstreamCancelCompleted, isTrue);

    first.add(1);
    second.add(2);
    await first.close();
    await second.close();

    expect(downstreamData, isEmpty);
  });

  test('mergeWith currently returns a single-subscription stream', () {
    final first = const Stream<int>.empty(broadcast: true);
    final second = const Stream<int>.empty(broadcast: true);

    final merged = first.mergeWith(second);

    expect(merged.isBroadcast, isFalse);
  });
}
