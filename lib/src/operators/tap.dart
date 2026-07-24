import 'dart:async';

extension RxdtStreamExtensions<T> on Stream<T> {
  Stream<T> tap(void Function(T event) onData) {
    return transform(_TapStreamTransformer<T>(onData));
  }
}

final class _TapStreamTransformer<T> extends StreamTransformerBase<T, T> {
  const _TapStreamTransformer(this._onData);

  final void Function(T event) _onData;

  @override
  Stream<T> bind(Stream<T> stream) {
    return stream.transform(
      StreamTransformer<T, T>.fromHandlers(
        handleData: (event, sink) {
          _onData(event);
          sink.add(event);
        },
      ),
    );
  }
}
