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
from datetime import datetime
from random import random
from typing import Any, Dict

import redis
from celery import __version__ as celeryVersion, Task, Celery
from celery.backends.redis import RedisBackend
from celery.exceptions import TimeoutError
from celery.local import PromiseProxy
from kombu import Connection
from twisted.internet import defer, reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.threads import deferToThreadPool
from twisted.python.failure import Failure

isCeleryV4 = celeryVersion.startswith("4.")

logger = logging.getLogger(__name__)

"""
DEBUGGING NOTES

watch "redis-cli client list | sed 's/idle=/idle /g;s/age=/age /g' | sort -r -n -k6 "

watch -n 0.1 "echo -n 'Redis: '; redis-cli client list | wc -l; echo -n 'RabbitMQ: '; netstat -an | grep 5672 | wc -l"

INSERT INTO pl_diagram."DispCompilerQueue" ("dispId")
SELECT id FROM pl_diagram."DispBase";

"""


class _ThreadConnection:
    MAX_CONN_PERIOD_SECONDS: int = None

    def __init__(self, app: Celery):

        self._threadId = threading.get_ident()
        self._app = app
        self._brokerUrl = app.conf["broker_url"]
        self._resultBackendUrl = app.conf["result_backend"]

        self._ampqConn = None
        self._redisConn = None
        self._connectedSince = None
        self._connectionMaxSeconds = None

    def _connectRedis(self):
        self._redisConn = RedisBackend(
            max_connection=1,
            url=self._resultBackendUrl,
            app=self._app
        )

    def _connectAmpq(self):
        self._ampqConn = Connection(self._brokerUrl)

    def initialise(self):
        if self._connectedSince:
            return
        self._connectedSince = datetime.utcnow()
        self._connectAmpq()
        self._connectRedis()

        # Add some salt to the number so that we don't end up reconnecting the whole
        # pool all at once.
        # This will be +/- 10 seconds
        self._connectionMaxSeconds = int((random() - 0.5) * 20.0) \
                                     + self.MAX_CONN_PERIOD_SECONDS

    def cleanup(self):
        if not self._connectedSince:
            return

        currentConnSeconds = (datetime.utcnow() - self._connectedSince).seconds
        if self._connectionMaxSeconds < currentConnSeconds:
            logger.debug("Closing connections for thread %s", self._threadId)
            self.shutdown()

    @property
    def ampqConn(self):
        assert self._threadId == threading.get_ident(), "AmpQ conn is not for this thread"
        return self._ampqConn

    @property
    def redisConn(self):
        assert self._threadId == threading.get_ident(), \
            "Redis conn is not for this thread"

        return self._redisConn

    def shutdown(self):
        if self._ampqConn:
            self._ampqConn.close()
            self._ampqConn = None

        if self._redisConn:
            client = self._redisConn.client
            client.pubsub().close()
            client.connection_pool.disconnect()
            self._redisConn = None

        self._connectedSince = None
        self._connectionMaxSeconds = None


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

    _threadConns: Dict[Any, _ThreadConnection] = {}

    @classmethod
    def startCeleryThreads(cls, threadCount=50, maxConnectionTime=120.0):
        """ Start Celery Threads

        Configure the Celery connection settings.

        :param threadCount: This is the size of the threadpool used to send to
            the celery worker
            and receive the results back from the celery worker.

        :param maxConnectionTime: This it the maximum time a connection will be used
            for before it's closed and a new one is opened.

        """
        _ThreadConnection.MAX_CONN_PERIOD_SECONDS = maxConnectionTime

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
        Celery.backend = cls._pathCeleryBackendProperty

        # Start the cleanup loop
        reactor.callLater(1.0, cls._scheduleCheckThreadConnCleanup)

    @classmethod
    def setReactorShuttingDown(cls):
        cls.__reactorShuttingDown = True

        if cls.__threadPool:
            cls.__threadPool.stop()
            cls.__threadPool = None

        while cls._threadConns:
            threadConn = cls._threadConns.popitem()[1]
            threadConn.shutdown()

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
    def _getThreadConns(cls, app: Celery):
        threadId = threading.get_ident()
        threadConn = cls._threadConns.get(threadId)

        if not threadConn:
            threadConn = _ThreadConnection(app)
            cls._threadConns[threadId] = threadConn

        threadConn.initialise()
        return threadConn

    @classmethod
    def _scheduleCheckThreadConnCleanup(cls):
        if not cls.__threadPool or cls.__reactorShuttingDown:
            return

        def err(failure):
            logger.exception(failure.value)

        for _ in range(cls.__threadPool.workers):
            d = deferToThreadPool(reactor, cls.__threadPool,
                                  cls._checkThreadConnCleanup)
            d.addErrback(err)

        reactor.callLater(60.0, cls._scheduleCheckThreadConnCleanup)

    @classmethod
    def _checkThreadConnCleanup(cls):
        """ Check Thread Connection Cleanup

        This method is periodically run to cleanup any connections that have not
        been used in a while.

        This method is run in the threadpool, so it will ensure that the running thread
        is not using the connection, simply because if it were, this method couldn't
        be run in this thread.

        """
        threadId = threading.get_ident()
        threadConn = cls._threadConns.get(threadId)
        if threadConn:
            threadConn.cleanup()

    @property
    def _pathCeleryBackendProperty(self):
        """ Patch the celery app backend method
        celery.app.base.py
        Celery.backend
        (line 1221)
        """
        # noinspection PyTypeChecker
        threadConn = _DeferredTask._getThreadConns(self)
        return threadConn.redisConn

    def _patchAsyncResult(self, task_id, **kwargs):
        """ Patch the celery task AsyncResult method
        celery.app.task.py
        Task.AsyncResult
        (line 782)
        """
        app = self._get_app()
        threadConn = _DeferredTask._getThreadConns(app)
        return app.AsyncResult(task_id,
                               backend=threadConn.redisConn,
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
        threadConn = self._getThreadConns(func.app)

        try:
            self.__asyncResult = func.apply_async(args=args, kwargs=kwargs,
                                                  connection=threadConn.ampqConn)

            if isinstance(self.__asyncResult, PromiseProxy):
                raise TypeError('Decorate with "DeferrableTask, not "_DeferredTask".')

            while True:
                if self.called or self.__reactorShuttingDown:
                    return

                try:
                    # We need to add a timeout here, otherwise the caller service
                    # can never shutdown while it's wiating for a task to complete
                    self.__asyncResult.get(timeout=5.0)
                    break

                except TimeoutError:
                    continue

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
            threadConn.cleanup()


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
