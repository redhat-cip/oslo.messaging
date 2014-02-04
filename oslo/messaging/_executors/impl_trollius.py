# Copyright 2014 Enovance.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from concurrent import futures
import logging
import sys
import threading

import asyncio

from oslo import messaging
from oslo.messaging._executors import base

_LOG = logging.getLogger(__name__)


class EventLoopThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.loop = None
        self._wait_start = threading.Event()

    def wait_start(self):
        self._wait_start.wait()

    def run(self):
        # create a loop in the dedicated thread and save it in the policy
        policy = asyncio.get_event_loop_policy()
        loop = policy.new_event_loop()
        policy.set_event_loop(loop)
        self.loop = loop

        # run the loop in the thread
        self.loop.call_soon(self._wait_start.set)
        try:
            self.loop.run_forever()
        except asyncio.CancelledError:
            pass


class TrolliusExecutor(base.ExecutorBase):

    def __init__(self, conf, listener, callback, event_loop=None):
        super(TrolliusExecutor, self).__init__(conf, listener, callback)
        if event_loop is None:
            self._thread = EventLoopThread()
            self.loop = None
        else:
            self.loop = event_loop
        self.thread_pool = futures.ThreadPoolExecutor(5)
        self._wait_stop = threading.Event()

    def _dispatch(self, incoming):
        try:
            result = self.callback(incoming.ctxt, incoming.message)
        except messaging.ExpectedException as e:
            _LOG.debug('Expected exception during message handling (%s)' %
                       e.exc_info[1])
            self.thread_pool.submit(incoming.reply,
                                    failure=e.exc_info, log_failure=False)
        except Exception:
            # sys.exc_info() is deleted by LOG.exception().
            exc_info = sys.exc_info()
            _LOG.error('Exception during message handling',
                       exc_info=exc_info)
            self.thread_pool.submit(incoming.reply, failure=exc_info)
        else:
            self.thread_pool.submit(incoming.reply, result)

    @asyncio.coroutine
    def _poll(self):
        try:
            while True:
                future = self.loop.run_in_executor(self.thread_pool,
                                                   self.listener.poll)
                try:
                    incoming = yield future
                except asyncio.CancelledError:
                    # FIXME: don't cancel, but stop the listener instead
                    future.cancel()
                    raise
                self._dispatch(incoming)
        finally:
            self._wait_stop.set()

    def _start_polling(self):
        self._poll_task = asyncio.async(self._poll(), loop=self.loop)

    def start(self):
        if self._thread is not None:
            self._thread.start()
            # wait until the event loop has been configured
            # (until the event loop is running)
            self._thread.wait_start()
            self.loop = self._thread.loop

        self.loop.call_soon_threadsafe(self._start_polling)

        self._wait_stop.wait()

    def stop(self):
        if self._poll_task is not None:
            self._poll_task.cancel()
            self._poll_task = None
        if self._thread is not None:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join()
        if self.thread_pool is not None:
            self.thread_pool.shutdown(wait=True)
            self.thread_pool = None

    def wait(self):
        pass
