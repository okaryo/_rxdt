import 'dart:async';

import 'package:test/test.dart';

Stream<int> _observedSource(List<String> events) async* {
  events.add('source:start');
  yield 1;
  events.add('source:after-yield');
}

void main() {
  test('an async generator does not start before it is listened to', () async {
    final sourceEvents = <String>[];
    final stream = _observedSource(sourceEvents);

    expect(sourceEvents, isEmpty);

    await stream.drain<void>();

    expect(sourceEvents, [
      'source:start',
      'source:after-yield',
    ]);
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
    expect(listenerEvents, [
      'listener:data:1',
      'listener:done',
    ]);
  });
}
