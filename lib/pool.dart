// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:async';
import 'dart:collection';

import 'package:async/async.dart';
import 'package:stack_trace/stack_trace.dart';

/// Manages an abstract pool of resources with a limit on how many may be in use
/// at once.
///
/// When a resource is needed, the user should call [request]. When the returned
/// future completes with a [PoolResource], the resource may be allocated. Once
/// the resource has been released, the user should call [PoolResource.release].
/// The pool will ensure that only a certain number of [PoolResource]s may be
/// allocated at once.
class Pool {
  /// Completers for requests beyond the first [_maxAllocatedResources].
  ///
  /// When an item is released, the next element of [_requestedResources] will
  /// be completed.
  final _requestedResources = Queue<Completer<PoolResource>>();

  /// Callbacks that must be called before additional resources can be
  /// allocated.
  ///
  /// See [PoolResource.allowRelease].
  final _onReleaseCallbacks = Queue<void Function()>();

  /// Completers that will be completed once `onRelease` callbacks are done
  /// running.
  ///
  /// These are kept in a queue to ensure that the earliest request completes
  /// first regardless of what order the `onRelease` callbacks complete in.
  final _onReleaseCompleters = Queue<Completer<PoolResource>>();

  /// The maximum number of resources that may be allocated at once.
  final int _maxAllocatedResources;

  /// The number of resources that are currently allocated.
  int _allocatedResources = 0;

  /// The timeout timer.
  ///
  /// This timer is canceled as long as the pool is below the resource limit.
  /// It's reset once the resource limit is reached and again every time an
  /// resource is released or a new resource is requested. If it fires, that
  /// indicates that the caller became deadlocked, likely due to files waiting
  /// for additional files to be read before they could be closed.
  ///
  /// This is `null` if this pool shouldn't time out.
  RestartableTimer _timer;

  /// The amount of time to wait before timing out the pending resources.
  final Duration _timeout;

  /// A [FutureGroup] that tracks all the `onRelease` callbacks for resources
  /// that have been marked releasable.
  ///
  /// This is `null` until [close] is called.
  FutureGroup _closeGroup;

  /// Whether [close] has been called.
  bool get isClosed => _closeMemo.hasRun;

  /// A future that completes once the pool is closed and all its outstanding
  /// resources have been released.
  ///
  /// If any [PoolResource.allowRelease] callback throws an exception after the
  /// pool is closed, this completes with that exception.
  Future get done => _closeMemo.future;

  /// Creates a new pool with the given limit on how many resources may be
  /// allocated at once.
  ///
  /// If [timeout] is passed, then if that much time passes without any activity
  /// all pending [request] futures will throw a [TimeoutException]. This is
  /// intended to avoid deadlocks.
  Pool(this._maxAllocatedResources, {Duration timeout}) : _timeout = timeout {
    if (timeout != null) {
      // Start the timer canceled since we only want to start counting down once
      // we've run out of available resources.
      _timer = RestartableTimer(timeout, _onTimeout)..cancel();
    }
  }

  /// Request a [PoolResource].
  ///
  /// If the maximum number of resources is already allocated, this will delay
  /// until one of them is released.
  Future<PoolResource> request() {
    if (isClosed) {
      throw StateError("request() may not be called on a closed Pool.");
    }

    if (_allocatedResources < _maxAllocatedResources) {
      _allocatedResources++;
      return Future.value(PoolResource._(this));
    } else if (_onReleaseCallbacks.isNotEmpty) {
      return _runOnRelease(_onReleaseCallbacks.removeFirst());
    } else {
      var completer = Completer<PoolResource>();
      _requestedResources.add(completer);
      _resetTimer();
      return completer.future;
    }
  }

  /// Requests a resource for the duration of [callback], which may return a
  /// Future.
  ///
  /// The return value of [callback] is piped to the returned Future.
  Future<T> withResource<T>(FutureOr<T> callback()) async {
    if (isClosed) {
      throw StateError("withResource() may not be called on a closed Pool.");
    }

    var resource = await request();
    try {
      return await callback();
    } finally {
      resource.release();
    }
  }

  /// Errors thrown from iterating [sourceItems] or because [isClosed] is `true`
  /// will not be passed to [onError] and will always be added to the returned
  /// stream.
  Stream<T> forEach<S, T>(Iterable<S> sourceItems, FutureOr<T> action(S source),
      {bool onError(S item, Object error, StackTrace stack)}) {
    onError ??= (item, e, s) => true;

    Completer cancelCompleter;

    var count = 1;

    bool canceled() => cancelCompleter != null;

    var controller = StreamController<T>(onCancel: () async {
      if (count > 0) {
        print('...canceling with count $count');
        assert(cancelCompleter == null);
        cancelCompleter = Completer();
        await cancelCompleter.future;
        print('cancel done!');
      }
    });

    var startedItems = 0;
    var finishedItems = 0;
    var skippedItems = 0;

    Future<void> finish() async {
      count--;
      if (canceled()) {
        print([count, startedItems, finishedItems, skippedItems]);
      }
      try {
        assert(count >= 0);
        if (count == 0) {
          print('closing');
          cancelCompleter?.complete();
          await controller.close();
          print('closed!');
        }
      } catch (e, stack) {
        Zone.current.handleUncaughtError(e, stack);
      }
    }

    void process() async {
      try {
        for (var item in sourceItems) {
          startedItems++;
          print('looking at $startedItems');

          assert(!canceled());
          final resource = await request();

          if (canceled()) {
            break;
          }
          count++;

          assert(!canceled());

          Timer.run(() async {
            try {
              if (canceled()) {
                print('skipping work!');
                skippedItems++;
                return;
              }
              T value;
              try {
                value = await action(item);
              } catch (e, stack) {
                assert(!canceled());
                if (onError(item, e, stack)) {
                  print('adding error');
                  controller.addError(e, stack);
                  print('added error');
                }
                return;
              }
              controller.add(value);
              finishedItems++;
            } catch (e, stack) {
              print('gah!');
              Zone.current.handleUncaughtError(e, stack);
            } finally {
              resource.release();
              await finish();
            }
          });
        }
      } catch (e, stack) {
        print('doh! $e');
        Zone.current.runBinaryGuarded(controller.addError, e, stack);
      } finally {
        print('all done!');
        await finish();
      }
    }

    Timer.run(process);

    return controller.stream;
  }

  /// Closes the pool so that no more resources are requested.
  ///
  /// Existing resource requests remain unchanged.
  ///
  /// Any resources that are marked as releasable using
  /// [PoolResource.allowRelease] are released immediately. Once all resources
  /// have been released and any `onRelease` callbacks have completed, the
  /// returned future completes successfully. If any `onRelease` callback throws
  /// an error, the returned future completes with that error.
  ///
  /// This may be called more than once; it returns the same [Future] each time.
  Future close() => _closeMemo.runOnce(() {
        if (_closeGroup != null) return _closeGroup.future;

        _resetTimer();

        _closeGroup = FutureGroup();
        for (var callback in _onReleaseCallbacks) {
          _closeGroup.add(Future.sync(callback));
        }

        _allocatedResources -= _onReleaseCallbacks.length;
        _onReleaseCallbacks.clear();

        if (_allocatedResources == 0) _closeGroup.close();
        return _closeGroup.future;
      });
  final _closeMemo = AsyncMemoizer();

  /// If there are any pending requests, this will fire the oldest one.
  void _onResourceReleased() {
    _resetTimer();

    if (_requestedResources.isNotEmpty) {
      var pending = _requestedResources.removeFirst();
      pending.complete(PoolResource._(this));
    } else {
      _allocatedResources--;
      if (isClosed && _allocatedResources == 0) _closeGroup.close();
    }
  }

  /// If there are any pending requests, this will fire the oldest one after
  /// running [onRelease].
  void _onResourceReleaseAllowed(onRelease()) {
    _resetTimer();

    if (_requestedResources.isNotEmpty) {
      var pending = _requestedResources.removeFirst();
      pending.complete(_runOnRelease(onRelease));
    } else if (isClosed) {
      _closeGroup.add(Future.sync(onRelease));
      _allocatedResources--;
      if (_allocatedResources == 0) _closeGroup.close();
    } else {
      var zone = Zone.current;
      var registered = zone.registerCallback(onRelease);
      _onReleaseCallbacks.add(() => zone.run(registered));
    }
  }

  /// Runs [onRelease] and returns a Future that completes to a resource once an
  /// [onRelease] callback completes.
  ///
  /// Futures returned by [_runOnRelease] always complete in the order they were
  /// created, even if earlier [onRelease] callbacks take longer to run.
  Future<PoolResource> _runOnRelease(onRelease()) {
    Future.sync(onRelease).then((value) {
      _onReleaseCompleters.removeFirst().complete(PoolResource._(this));
    }).catchError((error, StackTrace stackTrace) {
      _onReleaseCompleters.removeFirst().completeError(error, stackTrace);
    });

    var completer = Completer<PoolResource>.sync();
    _onReleaseCompleters.add(completer);
    return completer.future;
  }

  /// A resource has been requested, allocated, or released.
  void _resetTimer() {
    if (_timer == null) return;

    if (_requestedResources.isEmpty) {
      _timer.cancel();
    } else {
      _timer.reset();
    }
  }

  /// Handles [_timer] timing out by causing all pending resource completers to
  /// emit exceptions.
  void _onTimeout() {
    for (var completer in _requestedResources) {
      completer.completeError(
          TimeoutException(
              "Pool deadlock: all resources have been "
              "allocated for too long.",
              _timeout),
          Chain.current());
    }
    _requestedResources.clear();
    _timer = null;
  }
}

/// A member of a [Pool].
///
/// A [PoolResource] is a token that indicates that a resource is allocated.
/// When the associated resource is released, the user should call [release].
class PoolResource {
  final Pool _pool;

  /// Whether [this] has been released yet.
  bool _released = false;

  PoolResource._(this._pool);

  /// Tells the parent [Pool] that the resource associated with this resource is
  /// no longer allocated, and that a new [PoolResource] may be allocated.
  void release() {
    if (_released) {
      throw StateError("A PoolResource may only be released once.");
    }
    _released = true;
    _pool._onResourceReleased();
  }

  /// Tells the parent [Pool] that the resource associated with this resource is
  /// no longer necessary, but should remain allocated until more resources are
  /// needed.
  ///
  /// When [Pool.request] is called and there are no remaining available
  /// resources, the [onRelease] callback is called. It should free the
  /// resource, and it may return a Future or `null`. Once that completes, the
  /// [Pool.request] call will complete to a new [PoolResource].
  ///
  /// This is useful when a resource's main function is complete, but it may
  /// produce additional information later on. For example, an isolate's task
  /// may be complete, but it could still emit asynchronous errors.
  void allowRelease(onRelease()) {
    if (_released) {
      throw StateError("A PoolResource may only be released once.");
    }
    _released = true;
    _pool._onResourceReleaseAllowed(onRelease);
  }
}
