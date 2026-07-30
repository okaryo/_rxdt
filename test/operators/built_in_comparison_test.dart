import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test('mapValue matches Stream.map when conversion throws', () async {
    String convert(int value) {
      if (value == 2) {
        throw StateError('convert failed');
      }

      return 'value:$value';
    }

    final builtInEvents = await _record(
      Stream.fromIterable([1, 2, 3]).map(convert),
    );
    final rxdtEvents = await _record(
      Stream.fromIterable([1, 2, 3]).mapValue(convert),
    );

    expect(builtInEvents, ['data:value:1', 'error', 'data:value:3', 'done']);
    expect(rxdtEvents, builtInEvents);
  });

  test('filterValue matches Stream.where when predicate throws', () async {
    bool testValue(int value) {
      if (value == 2) {
        throw StateError('predicate failed');
      }

      return value.isOdd;
    }

    final builtInEvents = await _record(
      Stream.fromIterable([1, 2, 3]).where(testValue),
    );
    final rxdtEvents = await _record(
      Stream.fromIterable([1, 2, 3]).filterValue(testValue),
    );

    expect(builtInEvents, ['data:1', 'error', 'data:3', 'done']);
    expect(rxdtEvents, builtInEvents);
  });

  test(
    'distinctValue matches Stream.distinct when equality checking throws',
    () async {
      Future<List<String>> recordBuiltIn() {
        var shouldThrow = true;

        return _record(
          Stream.fromIterable([1, 2, 2]).distinct((previous, next) {
            if (next == 2 && shouldThrow) {
              shouldThrow = false;
              throw StateError('equality failed');
            }

            return previous == next;
          }),
        );
      }

      Future<List<String>> recordRxdt() {
        var shouldThrow = true;

        return _record(
          Stream.fromIterable([1, 2, 2]).distinctValue((previous, next) {
            if (next == 2 && shouldThrow) {
              shouldThrow = false;
              throw StateError('equality failed');
            }

            return previous == next;
          }),
        );
      }

      final builtInEvents = await recordBuiltIn();
      final rxdtEvents = await recordRxdt();

      expect(builtInEvents, ['data:1', 'error', 'data:2', 'done']);
      expect(rxdtEvents, builtInEvents);
    },
  );
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
