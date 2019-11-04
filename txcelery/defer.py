"""txcelery
Copyright Sentimens Research Group, LLC
2014
MIT License

Module Contents:
    - DeferredTask
    - CeleryClient
"""
import logging
import time
from builtins import ValueError
from functools import wraps
from types import MethodType, FunctionType

import celery
import redis
import twisted
from celery import __version__ as celeryVersion
from celery import states
from celery.local import PromiseProxy
from celery.result import AsyncResult
from celery.worker.control import revoke
from twisted.internet import defer, reactor
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure
from twisted.python.threadpool import ThreadPool

isCeleryV4 = celeryVersion.startswith("4.")

logger = logging.getLogger(__name__)


class _DeferredTask(defer.Deferred):
    """Subclass of `twisted.defer.Deferred` that wraps a
    `celery.local.PromiseProxy` (i.e. a "Celery task"), exposing the combined
    functionality of both classes.

    `_DeferredTask` instances can be treated both like ordinary Deferreds and
    oridnary PromiseProxies.
    """

    #: Wait Period
    POLL_PERIOD = 0.20
    WAIT_TIMEOUT = 1.000

    _ReactorShuttingDown = False

    @classmethod
    def setReactorShuttingDown(cls):
        cls._ReactorShuttingDown = True

    def __init__(self, func, *args, **kwargs):
        """Instantiate a `_DeferredTask`.  See `help(_DeferredTask)` for details
        pertaining to functionality.

        :param async_result : celery.result.AsyncResult
            AsyncResult to be monitored.  When completed or failed, the
            _DeferredTask will callback or errback, respectively.
        """
        # Deferred is an old-style class
        defer.Deferred.__init__(self, _DeferredTask._canceller)
        self.addErrback(self._cbErrback)

        self.__timeoutPeriod = self.WAIT_TIMEOUT
        self.__taskId = None

        d = deferToThread(self._run, func, *args, **kwargs)
        d.addBoth(self._threadFinishInMain)

    def addTimeout(self, timeout, clock, onTimeoutCancel=None):
        defer.Deferred.addTimeout(self, timeout, clock, onTimeoutCancel=onTimeoutCancel)
        self.__timeoutPeriod = max(1, timeout - 2)

    def _threadFinishInMain(self, result):
        if self.called:
            return

        if isinstance(result, Failure):
            self.errback(result)

        else:
            self.callback(result)

    def _canceller(self, *args):
        if self.__taskId is None:
            return
        revoke(self.__taskId, terminate=True)

    def _cbErrback(self, failure: Failure) -> Failure:
        if isinstance(failure.value, TimeoutError):
            self._canceller()

        return failure

    def _run(self, func, *args, **kwargs):
        async_result = func.delay(*args, **kwargs)
        self.__taskId = async_result.id

        if isinstance(async_result, PromiseProxy):
            raise TypeError('Decorate with "DeferrableTask, not "_DeferredTask".')

        """ Monitor Task In Thread

        The Celery task state must be checked in a thread, otherwise it blocks.

        This may stuff with Celerys connection to the result backend.
        I'm not sure how it manages that.

        """
        try:
            # Async task comes up with lots of celery errors :-|
            # return self._wait_for_task(async_result)
            return self._poll_for_task(async_result)

        finally:
            async_result.forget()

    def _wait_for_task(self, async_result):
        while not self.called and not self._ReactorShuttingDown:
            try:
                return async_result.wait(timeout=self.__timeoutPeriod)

            except celery.exceptions.TimeoutError:
                continue

            except redis.exceptions.ConnectionError:
                # redis.exceptions.ConnectionError:
                # Error 32 while writing to socket. Broken pipe.
                continue

            except redis.exceptions.TimeoutError:
                # redis.exceptions.TimeoutError:
                # Timeout reading from socket
                continue

            except AttributeError:
                # builtins.AttributeError: 'NoneType' object has no attribute 'sendall'
                continue

            except RuntimeError:
                # builtins.RuntimeError: dictionary changed size during iteration
                continue

            except Exception as e:
                logger.exception(e)
                continue

    def _poll_for_task(self, async_result):
        """ Monitor Task In Thread

        The Celery task state must be checked in a thread, otherwise it blocks.

        This may stuff with Celerys connection to the result backend.
        I'm not sure how it manages that.

        """

        while not self.called and not self._ReactorShuttingDown:
            try:
                state = async_result.state

                if state in states.UNREADY_STATES:
                    continue

                result = async_result.result
                # Ignore connection errors, it will retry on the next loop

            except redis.exceptions.ConnectionError:
                # redis.exceptions.ConnectionError:
                # Error 32 while writing to socket. Broken pipe.
                return False, None

            except AttributeError:
                # builtins.AttributeError: 'NoneType' object has no attribute 'sendall'
                return False, None

            except RuntimeError:
                # builtins.RuntimeError: dictionary changed size during iteration
                return False, None

            if state in states.UNREADY_STATES:
                pass

            elif state == 'SUCCESS':
                return result

            elif state == 'FAILURE':
                raise result

            elif state == 'REVOKED':
                raise defer.CancelledError('Task %s' % self.__taskId)

            else:
                raise ValueError('Cannot respond to `%s` state' % state)

            time.sleep(self.POLL_PERIOD)


class DeferrableTask:
    """Decorator class that wraps a celery task such that any methods
    returning an Celery `AsyncResult` instance are wrapped in a
    `_DeferredTask` instance.

    Instances of `DeferrableTask` expose all methods of the underlying Celery
    task.

    Usage:

        @DeferrableTask
        @app.task
        def my_task():
            # ...

    :Note:  The `@DeferrableTask` decorator must be the __top_most__ decorator.

            The `@DeferrableTask` decorator must be called __after__ the
           `@app.task` decorator, meaning that the former must be __above__
           the latter.
    """

    def __init__(self, fn):
        if isCeleryV4 and not isinstance(fn, PromiseProxy):
            raise TypeError('Wrapped function must be a Celery task.')

        self._fn = fn

    def __repr__(self):
        s = self._fn.__repr__().strip('<>')
        return '<CeleryClient {s}>'.format(s=s)

    # Used by the worker to actually call the method
    def __call__(self, *args, **kw):
        return self._fn(*args, **kw)

    # Used by the main python code to start a celery task on a worker
    def delay(self, *args, **kw):
        return _DeferredTask(self._fn, *args, **kw)


# Backwards compatibility
class CeleryClient(DeferrableTask):
    pass


reactor.addSystemEventTrigger('before', 'shutdown',
                              _DeferredTask.setReactorShuttingDown)

__all__ = [CeleryClient, _DeferredTask, DeferrableTask]
