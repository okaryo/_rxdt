import 'dart:async';

import 'package:test/test.dart';

void main() {
  test(
    'controller reports subscription lifecycle callbacks in order',
    () async {
      final lifecycleEvents = <String>[];
      final listened = Completer<void>();
      final paused = Completer<void>();
      final resumed = Completer<void>();
      final canceled = Completer<void>();
      final controller = StreamController<int>(
        onListen: () {
          lifecycleEvents.add('onListen');
          listened.complete();
        },
        onPause: () {
          lifecycleEvents.add('onPause');
          paused.complete();
        },
        onResume: () {
          lifecycleEvents.add('onResume');
          resumed.complete();
        },
        onCancel: () {
          lifecycleEvents.add('onCancel');
          canceled.complete();
        },
      );

      final subscription = controller.stream.listen((_) {});
      await listened.future;

      subscription.pause();
      await paused.future;

      subscription.resume();
      await resumed.future;

      await subscription.cancel();
      await canceled.future;
      await controller.close();

      expect(lifecycleEvents, ['onListen', 'onPause', 'onResume', 'onCancel']);
    },
  );

  test('subscription callbacks can be replaced while active', () async {
    final events = <String>[];
    final initialDataReceived = Completer<void>();
    final replacementDataReceived = Completer<void>();
    final replacementErrorReceived = Completer<void>();
    final replacementDoneReceived = Completer<void>();
    final expectedError = StateError('boom');
    final expectedStackTrace = StackTrace.current;
    Object? receivedError;
    StackTrace? receivedStackTrace;
    final controller = StreamController<int>();

    final subscription = controller.stream.listen(
      (value) {
        events.add('initial:data:$value');
        initialDataReceived.complete();
      },
      onError: (Object error, StackTrace stackTrace) {
        events.add('initial:error');
      },
      onDone: () {
        events.add('initial:done');
      },
    );

    controller.add(1);
    await initialDataReceived.future;

    subscription
      ..onData((value) {
        events.add('replacement:data:$value');
        replacementDataReceived.complete();
      })
      ..onError((Object error, StackTrace stackTrace) {
        receivedError = error;
        receivedStackTrace = stackTrace;
        events.add('replacement:error');
        replacementErrorReceived.complete();
      })
      ..onDone(() {
        events.add('replacement:done');
        replacementDoneReceived.complete();
      });

    controller.add(2);
    await replacementDataReceived.future;

    controller.addError(expectedError, expectedStackTrace);
    await replacementErrorReceived.future;

    subscription.onData(null);
    controller.add(3);
    await controller.close();
    await replacementDoneReceived.future;

    expect(events, [
      'initial:data:1',
      'replacement:data:2',
      'replacement:error',
      'replacement:done',
    ]);
    expect(receivedError, same(expectedError));
    expect(receivedStackTrace, same(expectedStackTrace));
  });
}
