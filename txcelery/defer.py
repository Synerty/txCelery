"""txcelery
Copyright Sentimens Research Group, LLC
2014
MIT License

Module Contents:
    - DeferredTask
    - CeleryClient
"""
import logging
from builtins import ValueError
from functools import wraps
from types import MethodType, FunctionType

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

isCeleryV4 = celeryVersion.startswith("4.")

logger = logging.getLogger(__name__)


class _DeferredTask(defer.Deferred):
    """Subclass of `twisted.defer.Deferred` that wraps a
    `celery.local.PromiseProxy` (i.e. a "Celery task"), exposing the combined
    functionality of both classes.

    `_DeferredTask` instances can be treated both like ordinary Deferreds and
    oridnary PromiseProxies.
    """

    #: Poll Period
    POLL_PERIOD = 0.05

    def __init__(self, async_result):
        """Instantiate a `_DeferredTask`.  See `help(_DeferredTask)` for details
        pertaining to functionality.

        :param async_result : celery.result.AsyncResult
            AsyncResult to be monitored.  When completed or failed, the
            _DeferredTask will callback or errback, respectively.
        """

        if isinstance(async_result, PromiseProxy):
            raise TypeError('Decarate with "DeferrableTask, not "_DeferredTask".')

        # Deferred is an old-style class
        defer.Deferred.__init__(self, _DeferredTask._canceller)

        self.task = async_result
        self._monitor_task()

    def _canceller(self):
        revoke(self.task.id, terminate=True)

    def _monitor_task(self):
        """ Monitor Task

        Periodically check on the progress of the celery task.

        """

        def _cb(arg):
            finished, result = arg
            if finished:
                return self.callback(result)

            reactor.callLater(self.POLL_PERIOD, self._monitor_task)

        d = deferToThread(self._monitor_task_in_thread)
        d.addCallback(_cb)
        d.addErrback(self.errback)  # Chain the errback

    def _monitor_task_in_thread(self):
        """ Monitor Task In Thread

        The Celery task state must be checked in a thread, otherwise it blocks.

        This may stuff with Celerys connection to the result backend.
        I'm not sure how it manages that.

        """
        try:
            state = self.task.state

            if state in states.UNREADY_STATES:
                return False, None

            result = self.task.result

        except redis.exceptions.ConnectionError as e:
            # Ignore connection errors, it will retry on the next loop
            return False, None

        if state == 'SUCCESS':
            return True, result

        elif state == 'FAILURE':
            raise result

        elif state == 'REVOKED':
            raise defer.CancelledError('Task %s' % self.task.id)

        else:
            raise ValueError('Cannot respond to `%s` state' % state)


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

    def __call__(self, *args, **kw):
        return self._fn(*args, **kw)

    def __getattr__(self, attr):
        attr = getattr(self._fn, attr)
        if isinstance(attr, MethodType) or isinstance(attr, FunctionType):
            return self._wrap(attr)
        return attr

    @staticmethod
    def _wrap(method):
        @wraps(method)
        def wrapper(*args, **kw):
            if not twisted.python.threadable.isInIOThread():
                raise Exception(
                    "txCelery methods can only be called from the reactors main thread")

            def _cb(res):
                if isinstance(res, AsyncResult):
                    return _DeferredTask(res)
                return res

            def _retriedMethod(*args, **kwargs):
                while True:
                    try:
                        return method(*args, **kwargs)

                    except redis.exceptions.ConnectionError as e:
                        logger.debug("Retrying Async task due to redis error, %s", str(e))

            d = deferToThread(_retriedMethod, *args, **kw)
            d.addCallback(_cb)
            return d

        return wrapper


# Backwards compatibility
class CeleryClient(DeferrableTask):
    pass


__all__ = [CeleryClient, _DeferredTask, DeferrableTask]
