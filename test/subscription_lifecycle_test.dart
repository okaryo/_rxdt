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
}
