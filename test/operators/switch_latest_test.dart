import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test(
    'switchLatest cancels the previous inner before listening to the next',
    () async {
      final firstListened = Completer<void>();
      final firstCancelStarted = Completer<void>();
      final allowFirstCleanup = Completer<void>();
      final secondListened = Completer<void>();
      final receivedOne = Completer<void>();
      final receivedTwo = Completer<void>();
      final done = Completer<void>();
      final outer = StreamController<Stream<int>>();
      final first = StreamController<int>(
        onListen: firstListened.complete,
        onCancel: () async {
          firstCancelStarted.complete();
          await allowFirstCleanup.future;
        },
      );
      final second = StreamController<int>(onListen: secondListened.complete);
      final downstreamEvents = <String>[];

      outer.stream.switchLatest().listen(
        (value) {
          downstreamEvents.add('data:$value');

          if (value == 1) {
            receivedOne.complete();
          } else if (value == 2) {
            receivedTwo.complete();
          }
        },
        onDone: () {
          downstreamEvents.add('done');
          done.complete();
        },
      );

      outer.add(first.stream);
      await firstListened.future;

      first.add(1);
      await receivedOne.future;

      outer.add(second.stream);
      await firstCancelStarted.future;

      expect(second.hasListener, isFalse);

      first.add(99);
      await Future<void>.delayed(Duration.zero);

      expect(downstreamEvents, ['data:1']);

      allowFirstCleanup.complete();
      await secondListened.future;

      second.add(2);
      await receivedTwo.future;
      await first.close();
      await outer.close();

      expect(done.isCompleted, isFalse);

      await second.close();
      await done.future;

      expect(downstreamEvents, ['data:1', 'data:2', 'done']);
    },
  );

  test('switchLatest closes when outer completes without an inner', () async {
    final events = await const Stream<Stream<int>>.empty()
        .switchLatest()
        .toList();

    expect(events, isEmpty);
  });
}
