import 'dart:async';

extension RxdtRecoverStreamExtensions<T> on Stream<T> {
  Stream<T> recoverValue(
    T Function(Object error, StackTrace stackTrace) recover,
  ) {
    return transform(_RecoverValueStreamTransformer<T>(recover));
  }
}

final class _RecoverValueStreamTransformer<T>
    extends StreamTransformerBase<T, T> {
  const _RecoverValueStreamTransformer(this._recover);

  final T Function(Object error, StackTrace stackTrace) _recover;

  @override
  Stream<T> bind(Stream<T> stream) {
    return stream.transform(
      StreamTransformer<T, T>.fromHandlers(
        handleError: (error, stackTrace, sink) {
          T recovered;

          try {
            recovered = _recover(error, stackTrace);
          } catch (recoveryError, recoveryStackTrace) {
            sink.addError(recoveryError, recoveryStackTrace);
            return;
          }

          sink.add(recovered);
        },
      ),
    );
  }
}
