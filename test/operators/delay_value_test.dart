import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test('delayValue defers data and waits before forwarding done', () async {
    final source = StreamController<int>(sync: true);
    final downstreamEvents = <String>[];
    final done = Completer<void>();

    source.stream
        .delayValue(const Duration(milliseconds: 10))
        .listen(
          (value) => downstreamEvents.add('data:$value'),
          onDone: () {
            downstreamEvents.add('done');
            done.complete();
          },
        );

    source.add(1);
    source.add(2);
    await source.close();

    expect(downstreamEvents, isEmpty);
    expect(done.isCompleted, isFalse);

    await done.future;

    expect(downstreamEvents, ['data:1', 'data:2', 'done']);
  });

  test(
    'delayValue does not listen to its source until downstream does',
    () async {
      final listened = Completer<void>();
      final source = StreamController<int>(onListen: listened.complete);

      final delayed = source.stream.delayValue(Duration.zero);

      expect(listened.isCompleted, isFalse);
      expect(source.hasListener, isFalse);

      final subscription = delayed.listen(null);

      expect(listened.isCompleted, isTrue);
      expect(source.hasListener, isTrue);

      await subscription.cancel();
      await source.close();
    },
  );

  test('delayValue currently returns a single-subscription stream', () {
    final source = const Stream<int>.empty(broadcast: true);

    final delayed = source.delayValue(Duration.zero);

    expect(delayed.isBroadcast, isFalse);
  });
}
