import 'dart:async';

import 'package:test/test.dart';

void main() {
  group('StreamController delivery timing', () {
    test('an asynchronous controller delivers after add returns', () async {
      final events = <String>[];
      final dataDelivered = Completer<void>();
      final controller = StreamController<int>();

      controller.stream.listen((value) {
        events.add('listener:data:$value');
        dataDelivered.complete();
      });

      events.add('before:add');
      controller.add(1);
      events.add('after:add');

      expect(events, ['before:add', 'after:add']);

      await dataDelivered.future;

      expect(events, ['before:add', 'after:add', 'listener:data:1']);

      await controller.close();
    });

    test('a synchronous controller delivers during add', () async {
      final events = <String>[];
      final controller = StreamController<int>(sync: true);

      controller.stream.listen((value) {
        events.add('listener:data:$value');
      });

      events.add('before:add');
      controller.add(1);
      events.add('after:add');

      expect(events, ['before:add', 'listener:data:1', 'after:add']);

      await controller.close();
    });
  });

  test(
    'an asynchronous controller delivers from the microtask queue',
    () async {
      final events = <String>[];
      final eventQueueTurnRan = Completer<void>();
      final controller = StreamController<int>();

      controller.stream.listen((value) {
        events.add('stream:data:$value');
        scheduleMicrotask(() {
          events.add('microtask:from-listener');
        });
      });

      events.add('sync:start');

      scheduleMicrotask(() {
        events.add('microtask:before-add');
      });

      controller.add(1);

      scheduleMicrotask(() {
        events.add('microtask:after-add');
      });

      Timer.run(() {
        events.add('event-queue:timer');
        eventQueueTurnRan.complete();
      });

      events.add('sync:end');

      expect(events, ['sync:start', 'sync:end']);

      await eventQueueTurnRan.future;

      expect(events, [
        'sync:start',
        'sync:end',
        'microtask:before-add',
        'stream:data:1',
        'microtask:after-add',
        'microtask:from-listener',
        'event-queue:timer',
      ]);

      await controller.close();
    },
  );
}
