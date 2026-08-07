import 'dart:async';

extension RxdtCombineLatestStreamExtensions<A> on Stream<A> {
  Stream<R> combineLatestWith<B, R>(
    Stream<B> other,
    R Function(A first, B second) combine,
  ) {
    StreamSubscription<A>? firstSubscription;
    StreamSubscription<B>? secondSubscription;
    late A latestFirst;
    late B latestSecond;
    var hasFirst = false;
    var hasSecond = false;
    var completedSourceCount = 0;
    late final StreamController<R> output;

    void emitIfReady() {
      if (!hasFirst || !hasSecond) {
        return;
      }

      R combined;

      try {
        combined = combine(latestFirst, latestSecond);
      } catch (error, stackTrace) {
        output.addError(error, stackTrace);
        return;
      }

      output.add(combined);
    }

    void handleFirst(A value) {
      latestFirst = value;
      hasFirst = true;
      emitIfReady();
    }

    void handleSecond(B value) {
      latestSecond = value;
      hasSecond = true;
      emitIfReady();
    }

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

    output = StreamController<R>(
      onListen: () {
        firstSubscription = listen(
          handleFirst,
          onError: addError,
          onDone: markSourceDone,
        );
        secondSubscription = other.listen(
          handleSecond,
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
