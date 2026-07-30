import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test('tap observes data without changing it', () async {
    final observed = <int>[];

    final values = await Stream.fromIterable([
      1,
      2,
      3,
    ]).tap(observed.add).toList();

    expect(observed, [1, 2, 3]);
    expect(values, [1, 2, 3]);
  });

  test('tap forwards data, error, and done in order', () async {
    final controller = StreamController<int>();
    final expectedError = StateError('boom');
    final expectedStackTrace = StackTrace.current;
    final tappedData = <int>[];
    final downstreamEvents = <String>[];
    final done = Completer<void>();
    Object? downstreamError;
    StackTrace? downstreamStackTrace;
    var doneCount = 0;

    controller.stream
        .tap(tappedData.add)
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
    controller.add(2);
    await controller.close();
    await done.future;

    expect(tappedData, [1, 2]);
    expect(downstreamEvents, ['data:1', 'error', 'data:2', 'done']);
    expect(downstreamError, same(expectedError));
    expect(downstreamStackTrace, same(expectedStackTrace));
    expect(doneCount, 1);
  });

  test('tap replaces a callback failure with an error and continues', () async {
    final expectedError = StateError('tap failed');
    final observed = <int>[];
    final downstreamEvents = <String>[];
    final done = Completer<void>();
    final controller = StreamController<int>();
    Object? downstreamError;
    StackTrace? downstreamStackTrace;

    controller.stream
        .tap((value) {
          if (value == 2) {
            throw expectedError;
          }

          observed.add(value);
        })
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

    controller.add(1);
    controller.add(2);
    controller.add(3);
    await controller.close();
    await done.future;

    expect(observed, [1, 3]);
    expect(downstreamEvents, ['data:1', 'error', 'data:3', 'done']);
    expect(downstreamError, same(expectedError));
    expect(downstreamStackTrace, isNotNull);
  });

  test('tap does not listen to its source until downstream listens', () async {
    var sourceListenCount = 0;
    late final StreamController<int> controller;
    controller = StreamController<int>(
      onListen: () {
        sourceListenCount++;
        unawaited(controller.close());
      },
    );

    final tapped = controller.stream.tap((_) {});

    expect(sourceListenCount, 0);
    expect(controller.hasListener, isFalse);

    await tapped.drain<void>();

    expect(sourceListenCount, 1);
  });

  test('tap propagates pause and resume to its source', () async {
    final sourceLifecycleEvents = <String>[];
    final sourceListened = Completer<void>();
    final sourcePaused = Completer<void>();
    final sourceResumed = Completer<void>();
    final controller = StreamController<int>(
      onListen: sourceListened.complete,
      onPause: () {
        sourceLifecycleEvents.add('onPause');
        sourcePaused.complete();
      },
      onResume: () {
        sourceLifecycleEvents.add('onResume');
        sourceResumed.complete();
      },
    );

    final subscription = controller.stream.tap((_) {}).listen((_) {});
    await sourceListened.future;

    subscription.pause();
    await sourcePaused.future;

    expect(controller.isPaused, isTrue);

    subscription.resume();
    await sourceResumed.future;

    expect(controller.isPaused, isFalse);

    await subscription.cancel();
    await controller.close();

    expect(sourceLifecycleEvents, ['onPause', 'onResume']);
  });

  test('tap propagates cancellation and waits for source cleanup', () async {
    final sourceCancelStarted = Completer<void>();
    final allowSourceCleanup = Completer<void>();
    final controller = StreamController<int>(
      onCancel: () async {
        sourceCancelStarted.complete();
        await allowSourceCleanup.future;
      },
    );
    final subscription = controller.stream.tap((_) {}).listen((_) {});
    var downstreamCancelCompleted = false;

    final downstreamCancel = subscription.cancel().then((_) {
      downstreamCancelCompleted = true;
    });

    await sourceCancelStarted.future;

    expect(downstreamCancelCompleted, isFalse);

    allowSourceCleanup.complete();
    await downstreamCancel;
    await controller.close();

    expect(downstreamCancelCompleted, isTrue);
  });

  test('tap delivers no events after downstream cancellation', () async {
    final controller = StreamController<int>();
    final tappedData = <int>[];
    final downstreamData = <int>[];
    final firstEventReceived = Completer<void>();
    final subscription = controller.stream.tap(tappedData.add).listen((value) {
      downstreamData.add(value);
      firstEventReceived.complete();
    });

    controller.add(1);
    await firstEventReceived.future;

    await subscription.cancel();

    controller.add(2);
    await controller.close();
    await Future<void>.delayed(Duration.zero);

    expect(tappedData, [1]);
    expect(downstreamData, [1]);
  });

  test('tap cancellation wins over queued events while cleanup runs', () async {
    final sourceCancelStarted = Completer<void>();
    final allowSourceCleanup = Completer<void>();
    final controller = StreamController<int>(
      onCancel: () async {
        sourceCancelStarted.complete();
        await allowSourceCleanup.future;
      },
    );
    final tappedData = <int>[];
    final downstreamEvents = <String>[];
    final subscription = controller.stream
        .tap(tappedData.add)
        .listen(
          (value) => downstreamEvents.add('data:$value'),
          onDone: () => downstreamEvents.add('done'),
        );

    controller.add(1);
    final closeFuture = controller.close();
    final cancelFuture = subscription.cancel();

    await sourceCancelStarted.future;
    await Future<void>.delayed(Duration.zero);

    expect(tappedData, isEmpty);
    expect(downstreamEvents, isEmpty);

    allowSourceCleanup.complete();
    await cancelFuture;
    await closeFuture;

    expect(tappedData, isEmpty);
    expect(downstreamEvents, isEmpty);
  });

  test('tap preserves a single-subscription source', () async {
    final controller = StreamController<int>();
    final tapped = controller.stream.tap((_) {});

    expect(tapped.isBroadcast, isFalse);

    final firstSubscription = tapped.listen((_) {});
    await firstSubscription.cancel();

    expect(() => tapped.listen((_) {}), throwsStateError);

    await controller.close();
  });

  test('tap preserves a broadcast source', () async {
    final tappedData = <int>[];
    final firstEvents = <int>[];
    final secondEvents = <int>[];
    final firstReceived = Completer<void>();
    final secondReceived = Completer<void>();
    final controller = StreamController<int>.broadcast();
    final tapped = controller.stream.tap(tappedData.add);

    expect(tapped.isBroadcast, isTrue);

    tapped.listen((value) {
      firstEvents.add(value);
      firstReceived.complete();
    });
    tapped.listen((value) {
      secondEvents.add(value);
      secondReceived.complete();
    });

    controller.add(1);
    await Future.wait([firstReceived.future, secondReceived.future]);
    await controller.close();

    expect(firstEvents, [1]);
    expect(secondEvents, [1]);
    expect(tappedData, [1, 1]);
  });
}
