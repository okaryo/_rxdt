import 'dart:async';

import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test('mergeWith forwards a controlled cross-source interleaving', () async {
    final first = StreamController<String>(sync: true);
    final second = StreamController<String>(sync: true);
    final output = first.stream.mergeWith(second.stream).toList();

    first.add('A1');
    second.add('B1');
    first.add('A2');
    second.add('B2');

    await first.close();
    await second.close();

    expect(await output, ['A1', 'B1', 'A2', 'B2']);
  });

  test(
    'combineLatestWith combines a controlled cross-source interleaving',
    () async {
      final first = StreamController<String>(sync: true);
      final second = StreamController<String>(sync: true);
      final output = first.stream
          .combineLatestWith(second.stream, (first, second) => '$first+$second')
          .toList();

      first.add('A1');
      second.add('B1');
      first.add('A2');
      second.add('B2');

      await first.close();
      await second.close();

      expect(await output, ['A1+B1', 'A2+B1', 'A2+B2']);
    },
  );
}
