import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test('filterValue forwards only data accepted by its predicate', () async {
    final testedValues = <int>[];

    final values = await Stream.fromIterable([1, 2, 3, 4]).filterValue((value) {
      testedValues.add(value);
      return value.isEven;
    }).toList();

    expect(testedValues, [1, 2, 3, 4]);
    expect(values, [2, 4]);
  });

  test('filterValue forwards error and done without testing them', () async {
    final expectedError = StateError('boom');
    final expectedStackTrace = StackTrace.current;
    final testedValues = <int>[];
    final downstreamEvents = <String>[];
    final done = Completer<void>();
    final controller = StreamController<int>();
    Object? downstreamError;
    StackTrace? downstreamStackTrace;

    controller.stream
        .filterValue((value) {
          testedValues.add(value);
          return value.isEven;
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
    controller.addError(expectedError, expectedStackTrace);
    controller.add(2);
    await controller.close();
    await done.future;

    expect(testedValues, [1, 2]);
    expect(downstreamEvents, ['error', 'data:2', 'done']);
    expect(downstreamError, same(expectedError));
    expect(downstreamStackTrace, same(expectedStackTrace));
  });

  test(
    'filterValue replaces a predicate failure with an error and continues',
    () async {
      final expectedError = StateError('predicate failed');
      final downstreamEvents = <String>[];
      final done = Completer<void>();
      final controller = StreamController<int>();
      Object? downstreamError;
      StackTrace? downstreamStackTrace;

      controller.stream
          .filterValue((value) {
            if (value == 2) {
              throw expectedError;
            }

            return value.isOdd;
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

      expect(downstreamEvents, ['data:1', 'error', 'data:3', 'done']);
      expect(downstreamError, same(expectedError));
      expect(downstreamStackTrace, isNotNull);
    },
  );

  test('filterValue preserves the source stream kind', () {
    final single = Stream.fromIterable([1]).filterValue((_) => true);
    final broadcast = const Stream<int>.empty(
      broadcast: true,
    ).filterValue((_) => true);

    expect(single.isBroadcast, isFalse);
    expect(broadcast.isBroadcast, isTrue);
  });
}
