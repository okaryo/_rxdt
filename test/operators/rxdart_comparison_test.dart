import 'dart:async';

import 'package:rxdart/rxdart.dart';
import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test('tap and RxDart doOnData observe and preserve ordinary data', () async {
    final tapObserved = <int>[];
    final doOnDataObserved = <int>[];

    final tapValues = await Stream.fromIterable([
      1,
      2,
      3,
    ]).tap(tapObserved.add).toList();
    final doOnDataValues = await Stream.fromIterable([
      1,
      2,
      3,
    ]).doOnData(doOnDataObserved.add).toList();

    expect(tapObserved, [1, 2, 3]);
    expect(doOnDataObserved, tapObserved);
    expect(tapValues, [1, 2, 3]);
    expect(doOnDataValues, tapValues);
  });

  test('tap and RxDart doOnData differ when their callback throws', () async {
    void observe(int value) {
      if (value == 2) {
        throw StateError('observation failed');
      }
    }

    final tapEvents = await _record(
      Stream.fromIterable([1, 2, 3]).tap(observe),
    );
    final doOnDataEvents = await _record(
      Stream.fromIterable([1, 2, 3]).doOnData(observe),
    );

    expect(tapEvents, ['data:1', 'error', 'data:3', 'done']);
    expect(doOnDataEvents, ['data:1', 'error', 'data:2', 'data:3', 'done']);
  });
}

Future<List<String>> _record<T>(Stream<T> stream) async {
  final events = <String>[];
  final done = Completer<void>();

  stream.listen(
    (value) => events.add('data:$value'),
    onError: (Object _) => events.add('error'),
    onDone: () {
      events.add('done');
      done.complete();
    },
  );

  await done.future;
  return events;
}
