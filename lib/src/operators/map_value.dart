import 'dart:async';

extension RxdtMapStreamExtensions<T> on Stream<T> {
  Stream<R> mapValue<R>(R Function(T event) convert) {
    return transform(_MapValueStreamTransformer<T, R>(convert));
  }
}

final class _MapValueStreamTransformer<T, R>
    extends StreamTransformerBase<T, R> {
  const _MapValueStreamTransformer(this._convert);

  final R Function(T event) _convert;

  @override
  Stream<R> bind(Stream<T> stream) {
    return stream.transform(
      StreamTransformer<T, R>.fromHandlers(
        handleData: (event, sink) {
          R converted;

          try {
            converted = _convert(event);
          } catch (error, stackTrace) {
            sink.addError(error, stackTrace);
            return;
          }

          sink.add(converted);
        },
      ),
    );
  }
}
