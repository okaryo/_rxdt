import 'dart:async';

extension RxdtConcatStreamExtensions<T> on Stream<T> {
  Stream<T> concatWith(Stream<T> next) async* {
    yield* this;
    yield* next;
  }
}
