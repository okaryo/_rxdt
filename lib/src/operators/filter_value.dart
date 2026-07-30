import 'dart:async';

extension RxdtFilterStreamExtensions<T> on Stream<T> {
  Stream<T> filterValue(bool Function(T event) test) {
    return transform(_FilterValueStreamTransformer<T>(test));
  }
}

final class _FilterValueStreamTransformer<T>
    extends StreamTransformerBase<T, T> {
  const _FilterValueStreamTransformer(this._test);

  final bool Function(T event) _test;

  @override
  Stream<T> bind(Stream<T> stream) {
    return stream.transform(
      StreamTransformer<T, T>.fromHandlers(
        handleData: (event, sink) {
          bool accepted;

          try {
            accepted = _test(event);
          } catch (error, stackTrace) {
            sink.addError(error, stackTrace);
            return;
          }

          if (accepted) {
            sink.add(event);
          }
        },
      ),
    );
  }
}
