import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test(
    'mapValue transforms each data event and changes the output type',
    () async {
      final Stream<String> mapped = Stream.fromIterable([
        1,
        2,
        3,
      ]).mapValue((value) => 'value:$value');

      expect(await mapped.toList(), ['value:1', 'value:2', 'value:3']);
    },
  );

  test('mapValue forwards error and done without changing them', () async {
    final expectedError = StateError('boom');
    final expectedStackTrace = StackTrace.current;
    final downstreamEvents = <String>[];
    final done = Completer<void>();
    final controller = StreamController<int>();
    Object? downstreamError;
    StackTrace? downstreamStackTrace;

    controller.stream
        .mapValue((value) => 'value:$value')
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

    expect(downstreamEvents, ['data:value:1', 'error', 'data:value:2', 'done']);
    expect(downstreamError, same(expectedError));
    expect(downstreamStackTrace, same(expectedStackTrace));
  });

  test('mapValue preserves the source stream kind', () {
    final single = Stream.fromIterable([1]).mapValue((value) => '$value');
    final broadcast = const Stream<int>.empty(
      broadcast: true,
    ).mapValue((value) => '$value');

    expect(single.isBroadcast, isFalse);
    expect(broadcast.isBroadcast, isTrue);
  });
}
