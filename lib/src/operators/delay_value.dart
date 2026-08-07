import 'dart:async';

extension RxdtDelayStreamExtensions<T> on Stream<T> {
  Stream<T> delayValue(Duration duration) {
    StreamSubscription<T>? sourceSubscription;
    final pendingTimers = <Timer>{};
    var sourceDone = false;
    var canceled = false;
    var outputClosing = false;
    late final StreamController<T> output;

    void maybeClose() {
      if (!canceled && !outputClosing && sourceDone && pendingTimers.isEmpty) {
        outputClosing = true;
        unawaited(output.close());
      }
    }

    void scheduleValue(T value) {
      late final Timer timer;
      timer = Timer(duration, () {
        pendingTimers.remove(timer);

        if (!canceled) {
          output.add(value);
        }

        maybeClose();
      });
      pendingTimers.add(timer);
    }

    Future<void> cancelSourceAndTimers() async {
      canceled = true;

      for (final timer in pendingTimers) {
        timer.cancel();
      }
      pendingTimers.clear();

      await sourceSubscription?.cancel();
    }

    output = StreamController<T>(
      onListen: () {
        sourceSubscription = listen(
          scheduleValue,
          onError: output.addError,
          onDone: () {
            sourceDone = true;
            maybeClose();
          },
        );
      },
      onPause: () {
        sourceSubscription?.pause();
      },
      onResume: () {
        sourceSubscription?.resume();
      },
      onCancel: cancelSourceAndTimers,
    );

    return output.stream;
  }
}
