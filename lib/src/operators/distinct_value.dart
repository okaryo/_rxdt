import 'dart:async';

extension RxdtDistinctStreamExtensions<T> on Stream<T> {
  Stream<T> distinctValue([bool Function(T previous, T next)? equals]) {
    return transform(_DistinctValueStreamTransformer<T>(equals));
  }
}

final class _DistinctValueStreamTransformer<T>
    extends StreamTransformerBase<T, T> {
  const _DistinctValueStreamTransformer(this._equals);

  final bool Function(T previous, T next)? _equals;

  @override
  Stream<T> bind(Stream<T> stream) {
    return Stream.eventTransformed(
      stream,
      (sink) => _DistinctValueEventSink<T>(sink, _equals),
    );
  }
}

final class _DistinctValueEventSink<T> implements EventSink<T> {
  _DistinctValueEventSink(this._outputSink, this._equals);

  final EventSink<T> _outputSink;
  final bool Function(T previous, T next)? _equals;

  var _hasPrevious = false;
  late T _previous;

  @override
  void add(T event) {
    if (_hasPrevious &&
        (_equals?.call(_previous, event) ?? _previous == event)) {
      return;
    }

    _previous = event;
    _hasPrevious = true;
    _outputSink.add(event);
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {
    _outputSink.addError(error, stackTrace);
  }

  @override
  void close() {
    _outputSink.close();
  }
}
