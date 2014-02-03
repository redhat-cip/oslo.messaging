# Copyright 2013 Red Hat, Inc.
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

import asyncio

from oslo import messaging
from oslo.messaging._executors import base

LOG = logging.getLogger(__name__)


class AsyncioExecutor(base.ExecutorBase):

    def __init__(self, conf, listener, callback, event_loop=None):
        super(AsyncioExecutor, self).__init__(conf, listener, callback)
        self._running = False
        self._task = None
        self._pending_tasks = []
        self._event_loop = event_loop or asyncio.get_event_loop()
        self._thread_pool = futures.ThreadPoolExecutor(max_workers=1)

    @asyncio.coroutine
    def callback_wrapper(self, callback, ctxt, message):
        return callback(ctxt, message)

    def _execute_callback(self, incoming):
        task = asyncio.Task(self.callback_wrapper(self.callback,
                                                  incoming.ctxt,
                                                  incoming.message))
        if self.callback.sender_expects_reply:
            incoming.acknowledge()

        def callback_done(task):
            try:
                reply = task.result()
                if reply and self.callback.sender_expects_reply:
                    incoming.reply(reply)
            except messaging.RequeueMessageException:
                LOG.debug('Requeue exception during message handling')
                incoming.requeue()
            except Exception:
                incoming.acknowledge()
                exc_info = sys.exc_info()
                LOG.error('Exception during message handling',
                          exc_info=exc_info)
                if self.callback.sender_expects_reply:
                    incoming.reply(failure=exc_info)
            else:
                if not self.callback.sender_expects_reply:
                    incoming.acknowledge()
            finally:
                self._pending_tasks.remove(task)

        task.add_done_callback(callback_done)

        self._pending_tasks.append(task)

    @asyncio.coroutine
    def _process_blocking(self, listener):
        while self._running:
            future = self._event_loop.run_in_executor(self._thread_pool,
                                                      listener.poll)
            res = yield future
            self._execute_callback(res)

    def start(self):
        if self._running:
            return
        self._running = True

        m = self._process_blocking

        task = asyncio.Task(m(self.listener))

        def poll_done(f):
            self._pending_tasks.remove(task)

        task.add_done_callback(poll_done)
        self._pending_tasks.append(task)

    def stop(self):
        self._running = False
        if hasattr(self.listener, 'fileno'):
            self.event_loop.remove_reader(self.listener.fileno())
        for t in self._pending_tasks:
            t.cancel()
        self._thread_pool.shutdown()

    def wait(self):
        asyncio.wait(self._pending_tasks)
        self._pending_tasks = []
        self._thread_pool.shutdown(wait=True)
