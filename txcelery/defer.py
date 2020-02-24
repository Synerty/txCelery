"""txcelery
Copyright Sentimens Research Group, LLC
2014
MIT License

Module Contents:
    - DeferredTask
    - CeleryClient
"""
import logging
import threading
from builtins import ValueError

import redis
from celery import __version__ as celeryVersion, Task, Celery
from celery.backends.redis import RedisBackend
from celery.local import PromiseProxy
from kombu import Connection
from twisted.internet import defer, reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.threads import deferToThreadPool
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

    #: Wait Period
    MAX_RETRIES = 3

    __reactorShuttingDown = False
    __threadPool = None

    _ampqConns = {}
    _redisConns = {}

    @classmethod
    def startCeleryThreads(cls, threadCount=50):
        from twisted.python import threadpool
        cls.__threadPool = threadpool.ThreadPool(threadCount, threadCount,
                                                 name="txcelery")

        reactor.addSystemEventTrigger(
            "before", "shutdown", cls.setReactorShuttingDown
        )

        cls.__threadPool.start()

        # Patch the Task.AsyncResult method.
        # So that it uses the redis backend for this thread.
        Task.AsyncResult = cls._patchAsyncResult

    @classmethod
    def setReactorShuttingDown(cls):
        cls.__reactorShuttingDown = True

        if cls.__threadPool:
            cls.__threadPool.stop()
            cls.__threadPool = None

        while cls._ampqConns:
            conn = cls._ampqConns.popitem()[1]
            conn.close()

    def __init__(self, func, *args, **kwargs):
        """Instantiate a `_DeferredTask`.  See `help(_DeferredTask)` for details
        pertaining to functionality.

        :param async_result : celery.result.AsyncResult
            AsyncResult to be monitored.  When completed or failed, the
            _DeferredTask will callback or errback, respectively.
        """
        # Deferred is an old-style class
        defer.Deferred.__init__(self, self._canceller)
        self.addErrback(self._cbErrback)

        # Auto initialise the the thread pool if the app hasn't already done so
        if not self.__threadPool:
            logger.debug("Auto initialising txcelery with 50 threads")
            self.startCeleryThreads()

        self.__retries = self.MAX_RETRIES
        self.__taskFinished = None
        self.__asyncResult = None

        d = self._start(func, *args, **kwargs)
        d.addBoth(self._threadFinishInMain)

    @classmethod
    def _getAmpqConn(cls, task: Task):
        threadId = threading.get_ident()
        if threadId in cls._ampqConns:
            return cls._ampqConns[threadId]

        cls._ampqConns[threadId] = Connection(task.app.conf["broker_url"])
        return cls._ampqConns[threadId]

    @classmethod
    def _getRedisConn(cls, app: Celery):
        threadId = threading.get_ident()
        if threadId in cls._redisConns:
            return cls._redisConns[threadId]

        cls._redisConns[threadId] = RedisBackend(
            max_connection=1,
            url=app.conf["result_backend"],
            app=app
        )
        return cls._redisConns[threadId]

    def _patchAsyncResult(self, task_id, **kwargs):
        """ Patch the celery task AsyncResult method
        celery.app.task.py
        Task.AsyncResult
        (line 782)
        """
        app = self._get_app()
        return app.AsyncResult(task_id,
                               backend=_DeferredTask._getRedisConn(app),
                               task_name=self.name, **kwargs)

    @inlineCallbacks
    def _start(self, func, *args, **kwargs):
        while self.__threadPool and self.__retries \
                and not self.called and not self.__reactorShuttingDown:
            self.__retries -= 1
            try:
                result = yield deferToThreadPool(reactor, self.__threadPool,
                                                 self._run, func, *args, **kwargs)
                return result

            except redis.exceptions.ConnectionError:
                # redis.exceptions.ConnectionError:
                # Error 32 while writing to socket. Broken pipe.
                if not self.__retries:
                    raise

            except Exception as e:

                print(e.__class__.__name__)
                print(e)
                if not self.__retries:
                    raise

    def addTimeout(self, timeout, clock, onTimeoutCancel=None):
        defer.Deferred.addTimeout(self, timeout, clock, onTimeoutCancel=onTimeoutCancel)

    def _threadFinishInMain(self, result):
        if self.called:
            return

        if isinstance(result, Failure):
            if result.check(redis.exceptions.ConnectionError) and self.__retries:
                self.__retries -= 1

            self.errback(result)

        else:
            self.callback(result)

    def _canceller(self, *args):
        if self.__asyncResult is None or self.__taskFinished is None:
            return
        self.__asyncResult.revoke(terminate=True)

    def _cbErrback(self, failure: Failure) -> Failure:
        if isinstance(failure.value, TimeoutError):
            self._canceller()

        return failure

    def _run(self, func, *args, **kwargs):
        """ Monitor Task In Thread

        The Celery task state must be checked in a thread, otherwise it blocks.

        This may stuff with Celerys connection to the result backend.
        I'm not sure how it manages that.

        """

        try:
            self.__asyncResult = func.apply_async(args=args, kwargs=kwargs,
                                                  connection=self._getAmpqConn(func))

            if isinstance(self.__asyncResult, PromiseProxy):
                raise TypeError('Decorate with "DeferrableTask, not "_DeferredTask".')

            if self.called or self.__reactorShuttingDown:
                return

            self.__asyncResult.get()
            self.__taskFinished = True
            state = self.__asyncResult.state
            result = self.__asyncResult.result

            if state == 'SUCCESS':
                return result

            elif state == 'FAILURE':
                raise result

            elif state == 'REVOKED':
                raise defer.CancelledError('Task %s' % self.__asyncResult.id)

            else:
                raise ValueError('Cannot respond to `%s` state' % state)

        finally:
            if self.__asyncResult:
                self.__asyncResult.forget()


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
