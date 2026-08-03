import 'dart:async';

extension RxdtSwitchLatestStreamExtensions<T> on Stream<Stream<T>> {
  Stream<T> switchLatest() {
    StreamSubscription<Stream<T>>? outerSubscription;
    StreamSubscription<T>? innerSubscription;
    Future<void>? switchFuture;
    var generation = 0;
    var outerDone = false;
    var innerActive = false;
    var downstreamPaused = false;
    var canceled = false;
    var outputClosing = false;
    late final StreamController<T> output;

    void maybeClose() {
      if (!canceled &&
          !outputClosing &&
          outerDone &&
          !innerActive &&
          switchFuture == null) {
        outputClosing = true;
        unawaited(output.close());
      }
    }

    Future<void> switchTo(Stream<T> next, int nextGeneration) async {
      final previousSubscription = innerSubscription;
      innerSubscription = null;
      innerActive = false;

      if (previousSubscription != null) {
        try {
          await previousSubscription.cancel();
        } catch (error, stackTrace) {
          if (!canceled) {
            output.addError(error, stackTrace);
          }
        }
      }

      if (canceled || nextGeneration != generation) {
        return;
      }

      try {
        innerActive = true;
        final subscription = next.listen(
          (value) {
            if (!canceled && nextGeneration == generation) {
              output.add(value);
            }
          },
          onError: (Object error, StackTrace stackTrace) {
            if (!canceled && nextGeneration == generation) {
              output.addError(error, stackTrace);
            }
          },
          onDone: () {
            if (!canceled && nextGeneration == generation) {
              innerActive = false;
              maybeClose();
            }
          },
        );
        innerSubscription = subscription;

        if (downstreamPaused) {
          subscription.pause();
        }
      } catch (error, stackTrace) {
        innerActive = false;
        output.addError(error, stackTrace);
      }
    }

    void handleOuterData(Stream<T> next) {
      final nextGeneration = ++generation;
      outerSubscription?.pause();

      final currentSwitch = switchTo(next, nextGeneration);
      switchFuture = currentSwitch;

      unawaited(
        currentSwitch.whenComplete(() {
          if (!canceled) {
            outerSubscription?.resume();
          }
          if (identical(switchFuture, currentSwitch)) {
            switchFuture = null;
          }
          maybeClose();
        }),
      );
    }

    Future<void> cancelSubscriptions() async {
      canceled = true;
      generation++;
      final cancellations = <Future<void>>[];

      if (outerSubscription case final subscription?) {
        cancellations.add(subscription.cancel());
      }
      if (innerSubscription case final subscription?) {
        cancellations.add(subscription.cancel());
      }
      if (switchFuture case final switching?) {
        cancellations.add(switching);
      }

      await Future.wait(cancellations);
    }

    output = StreamController<T>(
      onListen: () {
        outerSubscription = listen(
          handleOuterData,
          onError: output.addError,
          onDone: () {
            outerDone = true;
            maybeClose();
          },
        );
      },
      onPause: () {
        downstreamPaused = true;
        outerSubscription?.pause();
        innerSubscription?.pause();
      },
      onResume: () {
        downstreamPaused = false;
        outerSubscription?.resume();
        innerSubscription?.resume();
      },
      onCancel: cancelSubscriptions,
    );

    return output.stream;
  }
}
