import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test(
    'combineLatestWith emits only after both sources have a latest value',
    () async {
      final firstCombined = Completer<void>();
      final secondCombined = Completer<void>();
      final thirdCombined = Completer<void>();
      final done = Completer<void>();
      final first = StreamController<int>();
      final second = StreamController<int>();
      final downstreamEvents = <String>[];

      first.stream
          .combineLatestWith(second.stream, (first, second) => first + second)
          .listen(
            (value) {
              downstreamEvents.add('data:$value');

              switch (downstreamEvents.length) {
                case 1:
                  firstCombined.complete();
                case 2:
                  secondCombined.complete();
                case 3:
                  thirdCombined.complete();
              }
            },
            onDone: () {
              downstreamEvents.add('done');
              done.complete();
            },
          );

      first.add(1);
      await Future<void>.delayed(Duration.zero);

      expect(downstreamEvents, isEmpty);

      second.add(10);
      await firstCombined.future;

      first.add(2);
      await secondCombined.future;

      await first.close();

      expect(done.isCompleted, isFalse);

      second.add(20);
      await thirdCombined.future;
      await second.close();
      await done.future;

      expect(downstreamEvents, ['data:11', 'data:12', 'data:22', 'done']);
    },
  );

  test(
    'combineLatestWith forwards combiner failures and keeps latest state',
    () async {
      final expectedError = StateError('combine failed');
      final errorReceived = Completer<void>();
      final dataReceived = Completer<void>();
      final done = Completer<void>();
      final first = StreamController<int>();
      final second = StreamController<int>();
      final downstreamEvents = <String>[];
      Object? downstreamError;
      StackTrace? downstreamStackTrace;

      first.stream
          .combineLatestWith(second.stream, (first, second) {
            if (first == 1 && second == 10) {
              throw expectedError;
            }

            return first + second;
          })
          .listen(
            (value) {
              downstreamEvents.add('data:$value');
              dataReceived.complete();
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

      first.add(1);
      second.add(10);
      await errorReceived.future;

      second.add(20);
      await dataReceived.future;

      await first.close();
      await second.close();
      await done.future;

      expect(downstreamEvents, ['error', 'data:21', 'done']);
      expect(downstreamError, same(expectedError));
      expect(downstreamStackTrace, isNotNull);
    },
  );

  test(
    'combineLatestWith stays empty if one source completes without data',
    () async {
      final first = StreamController<int>();
      final second = StreamController<int>();
      final downstreamEvents = <String>[];
      final done = Completer<void>();

      first.stream
          .combineLatestWith(second.stream, (first, second) => first + second)
          .listen(
            (value) => downstreamEvents.add('data:$value'),
            onDone: () {
              downstreamEvents.add('done');
              done.complete();
            },
          );

      await first.close();

      expect(done.isCompleted, isFalse);

      second.add(10);
      await second.close();
      await done.future;

      expect(downstreamEvents, ['done']);
    },
  );

  test('combineLatestWith currently returns a single-subscription stream', () {
    final first = const Stream<int>.empty(broadcast: true);
    final second = const Stream<int>.empty(broadcast: true);

    final combined = first.combineLatestWith(
      second,
      (first, second) => first + second,
    );

    expect(combined.isBroadcast, isFalse);
  });
}
