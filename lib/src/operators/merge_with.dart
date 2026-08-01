import 'dart:async';

extension RxdtMergeStreamExtensions<T> on Stream<T> {
  Stream<T> mergeWith(Stream<T> other) {
    StreamSubscription<T>? firstSubscription;
    StreamSubscription<T>? secondSubscription;
    var completedSourceCount = 0;
    late final StreamController<T> output;

    void addError(Object error, StackTrace stackTrace) {
      output.addError(error, stackTrace);
    }

    void markSourceDone() {
      completedSourceCount++;

      if (completedSourceCount == 2) {
        unawaited(output.close());
      }
    }

    Future<void> cancelSources() async {
      final cancellations = <Future<void>>[];

      if (firstSubscription case final subscription?) {
        cancellations.add(subscription.cancel());
      }
      if (secondSubscription case final subscription?) {
        cancellations.add(subscription.cancel());
      }

      await Future.wait(cancellations);
    }

    output = StreamController<T>(
      onListen: () {
        firstSubscription = listen(
          output.add,
          onError: addError,
          onDone: markSourceDone,
        );
        secondSubscription = other.listen(
          output.add,
          onError: addError,
          onDone: markSourceDone,
        );
      },
      onPause: () {
        firstSubscription?.pause();
        secondSubscription?.pause();
      },
      onResume: () {
        firstSubscription?.resume();
        secondSubscription?.resume();
      },
      onCancel: cancelSources,
    );

    return output.stream;
  }
}
