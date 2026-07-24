import 'dart:async';

import 'package:test/test.dart';

Stream<int> _observedSource(List<String> events) async* {
  events.add('source:start');
  yield 1;
  events.add('source:after-yield');
}

Stream<int> _observedErrorSource(Object error) async* {
  yield 1;
  throw error;
}

Stream<int> _gatedSource(
  Completer<void> reachedGate,
  Future<void> releaseGate,
) async* {
  yield 1;
  reachedGate.complete();
  await releaseGate;
  yield 2;
}

Never _throwSynchronously(Object error) => throw error;

void main() {
  test('an async generator does not start before it is listened to', () async {
    final sourceEvents = <String>[];
    final stream = _observedSource(sourceEvents);

    expect(sourceEvents, isEmpty);

    await stream.drain<void>();

    expect(sourceEvents, ['source:start', 'source:after-yield']);
  });

  test('listener callbacks run only after listen returns', () async {
    final listenerEvents = <String>[];
    final callbackStates = <bool>[];
    final done = Completer<void>();
    var listenReturned = false;

    _observedSource(<String>[]).listen(
      (value) {
        callbackStates.add(listenReturned);
        listenerEvents.add('listener:data:$value');
      },
      onDone: () {
        callbackStates.add(listenReturned);
        listenerEvents.add('listener:done');
        done.complete();
      },
    );

    listenReturned = true;

    expect(listenerEvents, isEmpty);

    await done.future;

    expect(callbackStates, everyElement(isTrue));
    expect(listenerEvents, ['listener:data:1', 'listener:done']);
  });

  test('listener receives data, error, and done in order', () async {
    final expectedError = StateError('boom');
    final listenerEvents = <String>[];
    final done = Completer<void>();
    Object? observedError;
    StackTrace? observedStackTrace;

    _observedErrorSource(expectedError).listen(
      (value) => listenerEvents.add('listener:data:$value'),
      onError: (Object error, StackTrace stackTrace) {
        observedError = error;
        observedStackTrace = stackTrace;
        listenerEvents.add('listener:error');
      },
      onDone: () {
        listenerEvents.add('listener:done');
        done.complete();
      },
      cancelOnError: false,
    );

    await done.future;

    expect(listenerEvents, [
      'listener:data:1',
      'listener:error',
      'listener:done',
    ]);
    expect(observedError, same(expectedError));
    expect(observedStackTrace, isNotNull);
  });

  test('a synchronous exception is thrown to its caller', () {
    final expectedError = StateError('outside stream');

    expect(
      () => _throwSynchronously(expectedError),
      throwsA(same(expectedError)),
    );
  });

  test('a stream error is delivered to onError instead of listen', () async {
    final expectedError = StateError('inside stream');
    final done = Completer<void>();
    Object? caughtAroundListen;
    Object? observedError;

    try {
      _observedErrorSource(expectedError).listen(
        (_) {},
        onError: (Object error) {
          observedError = error;
        },
        onDone: done.complete,
        cancelOnError: false,
      );
    } catch (error) {
      caughtAroundListen = error;
    }

    expect(caughtAroundListen, isNull);

    await done.future;

    expect(observedError, same(expectedError));
  });

  test('toList listens immediately and completes after done', () async {
    final reachedGate = Completer<void>();
    final releaseGate = Completer<void>();
    List<int>? completedValues;

    final valuesFuture = _gatedSource(reachedGate, releaseGate.future).toList();
    valuesFuture.then((values) {
      completedValues = values;
    });

    await reachedGate.future;

    expect(completedValues, isNull);

    releaseGate.complete();

    expect(await valuesFuture, [1, 2]);
  });

  test('toList completes with an error instead of a partial list', () async {
    final expectedError = StateError('collection failed');

    await expectLater(
      _observedErrorSource(expectedError).toList(),
      throwsA(same(expectedError)),
    );
  });
}
