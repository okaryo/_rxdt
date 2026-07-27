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

  group('reentrant event production', () {
    test('a synchronous broadcast controller rejects reentrant add', () async {
      final events = <String>[];
      Object? reentrantError;
      final controller = StreamController<int>.broadcast(sync: true);

      controller.stream.listen((value) {
        events.add('listener:start:$value');

        try {
          controller.add(value + 1);
        } on StateError catch (error) {
          reentrantError = error;
          events.add('reentrant:add:rejected');
        }

        events.add('listener:end:$value');
      });

      events.add('producer:before-add');
      controller.add(1);
      events.add('producer:after-add');

      expect(events, [
        'producer:before-add',
        'listener:start:1',
        'reentrant:add:rejected',
        'listener:end:1',
        'producer:after-add',
      ]);
      expect(reentrantError, isA<StateError>());

      await controller.close();
    });

    test(
      'an asynchronous broadcast controller queues add from listener',
      () async {
        final events = <String>[];
        final secondEventDelivered = Completer<void>();
        final controller = StreamController<int>.broadcast();

        controller.stream.listen((value) {
          events.add('listener:data:$value');

          if (value == 1) {
            controller.add(2);
          } else {
            secondEventDelivered.complete();
          }
        });

        events.add('producer:before-add');
        controller.add(1);
        events.add('producer:after-add');

        expect(events, ['producer:before-add', 'producer:after-add']);

        await secondEventDelivered.future;

        expect(events, [
          'producer:before-add',
          'producer:after-add',
          'listener:data:1',
          'listener:data:2',
        ]);

        await controller.close();
      },
    );
  });

  group('stream kinds', () {
    test('a single-subscription stream can only be listened to once', () async {
      final controller = StreamController<int>();

      expect(controller.stream.isBroadcast, isFalse);

      final firstSubscription = controller.stream.listen((_) {});
      await firstSubscription.cancel();

      expect(() => controller.stream.listen((_) {}), throwsStateError);

      await controller.close();
    });

    test('a broadcast stream accepts multiple subscriptions', () async {
      final controller = StreamController<int>.broadcast();

      expect(controller.stream.isBroadcast, isTrue);

      final firstSubscription = controller.stream.listen((_) {});
      final secondSubscription = controller.stream.listen((_) {});

      await firstSubscription.cancel();
      await secondSubscription.cancel();
      await controller.close();
    });

    test('a broadcast stream only delivers to current listeners', () async {
      final firstEvents = <int>[];
      final secondEvents = <int>[];
      final firstReceivedOne = Completer<void>();
      final firstReceivedTwo = Completer<void>();
      final secondReceivedTwo = Completer<void>();
      final secondReceivedThree = Completer<void>();
      final controller = StreamController<int>.broadcast();

      controller.add(0);

      final firstSubscription = controller.stream.listen((value) {
        firstEvents.add(value);

        if (value == 1) {
          firstReceivedOne.complete();
        } else if (value == 2) {
          firstReceivedTwo.complete();
        }
      });

      controller.add(1);
      await firstReceivedOne.future;

      controller.stream.listen((value) {
        secondEvents.add(value);

        if (value == 2) {
          secondReceivedTwo.complete();
        } else if (value == 3) {
          secondReceivedThree.complete();
        }
      });

      controller.add(2);
      await Future.wait([firstReceivedTwo.future, secondReceivedTwo.future]);

      await firstSubscription.cancel();

      controller.add(3);
      await secondReceivedThree.future;
      await controller.close();

      expect(firstEvents, [1, 2]);
      expect(secondEvents, [2, 3]);
    });

    test('a cold source starts a fresh producer for each consumer', () async {
      var productionStartCount = 0;

      Stream<int> createColdStream() async* {
        productionStartCount++;
        yield 1;
        yield 2;
      }

      expect(productionStartCount, 0);

      final firstEvents = await createColdStream().toList();
      expect(productionStartCount, 1);

      final secondEvents = await createColdStream().toList();
      expect(productionStartCount, 2);

      expect(firstEvents, [1, 2]);
      expect(secondEvents, [1, 2]);
    });

    test(
      'a hot shared stream has one producer and late listeners join midway',
      () async {
        var productionStartCount = 0;
        final allowSecondEvent = Completer<void>();
        final firstReceivedOne = Completer<void>();
        final firstDone = Completer<void>();
        final secondDone = Completer<void>();
        final firstEvents = <int>[];
        final secondEvents = <int>[];

        Stream<int> createSource() async* {
          productionStartCount++;
          yield 1;
          await allowSecondEvent.future;
          yield 2;
        }

        final sharedStream = createSource().asBroadcastStream();

        sharedStream.listen((value) {
          firstEvents.add(value);
          if (value == 1) {
            firstReceivedOne.complete();
          }
        }, onDone: firstDone.complete);
        await firstReceivedOne.future;

        sharedStream.listen(secondEvents.add, onDone: secondDone.complete);

        allowSecondEvent.complete();
        await Future.wait([firstDone.future, secondDone.future]);

        expect(productionStartCount, 1);
        expect(firstEvents, [1, 2]);
        expect(secondEvents, [2]);
      },
    );
  });
}
