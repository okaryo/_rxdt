import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test('distinctValue drops only consecutive duplicate data', () async {
    final values = await Stream.fromIterable([
      1,
      1,
      2,
      2,
      1,
      1,
    ]).distinctValue().toList();

    expect(values, [1, 2, 1]);
  });

  test('distinctValue preserves state across error events', () async {
    final expectedError = StateError('boom');
    final expectedStackTrace = StackTrace.current;
    final downstreamEvents = <String>[];
    final done = Completer<void>();
    final controller = StreamController<int>();
    Object? downstreamError;
    StackTrace? downstreamStackTrace;

    controller.stream.distinctValue().listen(
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
    controller.addError(expectedError, expectedStackTrace);
    controller.add(1);
    controller.add(2);
    await controller.close();
    await done.future;

    expect(downstreamEvents, ['data:1', 'error', 'data:2', 'done']);
    expect(downstreamError, same(expectedError));
    expect(downstreamStackTrace, same(expectedStackTrace));
  });

  test('distinctValue preserves the source stream kind', () {
    final single = Stream.fromIterable([1]).distinctValue();
    final broadcast = const Stream<int>.empty(broadcast: true).distinctValue();

    expect(single.isBroadcast, isFalse);
    expect(broadcast.isBroadcast, isTrue);
  });

  test('distinctValue owns previous-value state per subscription', () async {
    final firstEvents = <int>[];
    final secondEvents = <int>[];
    final firstReceivedOne = Completer<void>();
    final secondReceivedOne = Completer<void>();
    final firstReceivedTwo = Completer<void>();
    final secondReceivedTwo = Completer<void>();
    final controller = StreamController<int>.broadcast();
    final distinct = controller.stream.distinctValue();

    distinct.listen((value) {
      firstEvents.add(value);

      if (value == 1) {
        firstReceivedOne.complete();
      } else if (value == 2) {
        firstReceivedTwo.complete();
      }
    });

    controller.add(1);
    await firstReceivedOne.future;

    distinct.listen((value) {
      secondEvents.add(value);

      if (value == 1) {
        secondReceivedOne.complete();
      } else if (value == 2) {
        secondReceivedTwo.complete();
      }
    });

    controller.add(1);
    await secondReceivedOne.future;

    expect(firstEvents, [1]);
    expect(secondEvents, [1]);

    controller.add(2);
    await Future.wait([firstReceivedTwo.future, secondReceivedTwo.future]);
    await controller.close();

    expect(firstEvents, [1, 2]);
    expect(secondEvents, [1, 2]);
  });
}
