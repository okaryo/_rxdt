import 'package:rxdt/rxdt.dart';
import 'package:test/test.dart';

void main() {
  test('tap observes data without changing it', () async {
    final observed = <int>[];

    final values = await Stream.fromIterable([
      1,
      2,
      3,
    ]).tap(observed.add).toList();

    expect(observed, [1, 2, 3]);
    expect(values, [1, 2, 3]);
  });
}
