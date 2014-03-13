# Copyright (C) 2014 eNovance SAS <licensing@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import random
import thread
import threading
import time

import mock
import qpid
import testscenarios

from oslo.config import cfg
from oslo import messaging
from oslo.messaging._drivers import impl_qpid as qpid_driver
from tests import utils as test_utils


load_tests = testscenarios.load_tests_apply_scenarios

QPID_BROKER = 'localhost:5672'


class TestQpidDriverLoad(test_utils.BaseTestCase):

    def setUp(self):
        super(TestQpidDriverLoad, self).setUp()
        self.messaging_conf.transport_driver = 'qpid'

    def test_driver_load(self):
        transport = messaging.get_transport(self.conf)
        self.assertIsInstance(transport._driver, qpid_driver.QpidDriver)


def _is_qpidd_service_running():

    """this function checks if the qpid service is running or not."""

    qpid_running = True
    try:
        broker = QPID_BROKER
        connection = qpid.messaging.Connection(broker)
        connection.open()
    except Exception:
        # qpid service is not runnung.
        qpid_running = False
    else:
        connection.close()

    return qpid_running


class TestQpidTransportURL(test_utils.BaseTestCase):

    scenarios = [
        ('none', dict(url=None,
                      expected=[dict(host='localhost:5672',
                                     username='',
                                     password='')])),
        ('empty',
         dict(url='qpid:///',
              expected=[dict(host='localhost:5672',
                             username='',
                             password='')])),
        ('localhost',
         dict(url='qpid://localhost/',
              expected=[dict(host='localhost:5672',
                             username='',
                             password='')])),
        ('no_creds',
         dict(url='qpid://host/',
              expected=[dict(host='host:5672',
                             username='',
                             password='')])),
        ('no_port',
         dict(url='qpid://user:password@host/',
              expected=[dict(host='host:5672',
                             username='user',
                             password='password')])),
        ('full_url',
         dict(url='qpid://user:password@host:10/',
              expected=[dict(host='host:10',
                             username='user',
                             password='password')])),
        ('full_two_url',
         dict(url='qpid://user:password@host:10,'
              'user2:password2@host2:12/',
              expected=[dict(host='host:10',
                             username='user',
                             password='password'),
                        dict(host='host2:12',
                             username='user2',
                             password='password2')
                        ]
              )),

    ]

    def setUp(self):
        super(TestQpidTransportURL, self).setUp()

        self.messaging_conf.transport_driver = 'qpid'
        self._fake_qpid = not _is_qpidd_service_running()
        self._server_params = []
        cnx_init = qpid_driver.Connection.__init__

        def record_params(cnx, conf, url):
            if self._fake_qpid:
                with mock.patch('qpid.messaging.Connection') \
                        as Connection:
                    conn = Connection.return_value
                    conn.session.return_value = get_fake_qpid_session()
                    cnx_init(cnx, conf, url)
            else:
                cnx_init(cnx, conf, url)
            self._server_params = cnx.brokers[:]

        def dummy_send(cnx, topic, msg, timeout=None, retry=None):
            pass

        self.stubs.Set(qpid_driver.Connection, '__init__', record_params)
        self.stubs.Set(qpid_driver.Connection, 'topic_send', dummy_send)

        self._driver = messaging.get_transport(self.conf, self.url)._driver
        self._target = messaging.Target(topic='testtopic')

    def test_transport_url_listen(self):
        self._driver.listen(self._target)
        self.assertEqual(self._server_params, self.expected)

    def test_transport_url_listen_for_notification(self):
        self._driver.listen_for_notifications(
            [(messaging.Target(topic='topic'), 'info')])
        self.assertEqual(self._server_params, self.expected)

    def test_transport_url_send(self):
        self._driver.send(self._target, {}, {})
        self.assertEqual(self._server_params, self.expected)


class TestQpidInvalidTopologyVersion(test_utils.BaseTestCase):
    """Unit test cases to test invalid qpid topology version."""

    scenarios = [
        ('direct', dict(consumer_cls=qpid_driver.DirectConsumer,
                        publisher_cls=qpid_driver.DirectPublisher)),
        ('topic', dict(consumer_cls=qpid_driver.TopicConsumer,
                       publisher_cls=qpid_driver.TopicPublisher)),
        ('fanout', dict(consumer_cls=qpid_driver.FanoutConsumer,
                        publisher_cls=qpid_driver.FanoutPublisher)),
    ]

    def setUp(self):
        super(TestQpidInvalidTopologyVersion, self).setUp()

        self.qpid_opts = [
            cfg.BoolOpt('amqp_durable_queues',
                        default=False,
                        deprecated_name='rabbit_durable_queues',
                        deprecated_group='DEFAULT',
                        help='Use durable queues in amqp.'),
            cfg.BoolOpt('amqp_auto_delete',
                        default=False,
                        help='Auto-delete queues in amqp.'),
            cfg.IntOpt('qpid_topology_version',
                       default=-1,
                       help='qpid topology version'),
            cfg.StrOpt('control_exchange',
                       default='openstack',
                       help='AMQP exchange to connect to if using Qpid'),
        ]

        self.qpid_conf = cfg.ConfigOpts()
        self.qpid_conf.register_opts(self.qpid_opts)

        self.fake_qpid = not _is_qpidd_service_running()

        if self.fake_qpid:
            self.session = get_fake_qpid_session()
        else:
            self.broker = QPID_BROKER
            # create connection from the qpid.messaging
            self.connection = qpid.messaging.Connection(self.broker)
            self.connection.open()
            self.session = self.connection.session()

    def tearDown(self):
        self.qpid_conf.unregister_opts(self.qpid_opts)
        super(TestQpidInvalidTopologyVersion, self).tearDown()

        if self.fake_qpid:
            _fake_session.flush_exchanges()
        else:
            self.connection.close()

    def test_invalid_topology_version(self):
        def consumer_callback(msg):
            pass

        msgid_or_topic = 'test'

        # not using self.assertRaises because
        # 1. qpid driver raises Exception(msg) for invalid topology version
        # 2. flake8 - H202 assertRaises Exception too broad
        exception_msg = ("Invalid value for qpid_topology_version: %d" %
                         self.qpid_conf.qpid_topology_version)
        recvd_exc_msg = ''

        try:
            self.consumer_cls(self.qpid_conf, self.session, msgid_or_topic,
                              consumer_callback)
        except Exception as e:
            recvd_exc_msg = e.message

        self.assertEqual(exception_msg, recvd_exc_msg)

        recvd_exc_msg = ''
        try:
            self.publisher_cls(self.qpid_conf, self.session, msgid_or_topic)
        except Exception as e:
            recvd_exc_msg = e.message

        self.assertEqual(exception_msg, recvd_exc_msg)


class TestQpidDirectConsumerPublisher(test_utils.BaseTestCase):
    """Unit test cases to test DirectConsumer and Direct Publisher."""

    _n_qpid_topology = [
        ('v1', dict(qpid_topology=1)),
        ('v2', dict(qpid_topology=2)),
    ]

    _n_msgs = [
        ('single', dict(no_msgs=1)),
        ('multiple', dict(no_msgs=10)),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._n_qpid_topology,
                                                         cls._n_msgs)

    def setUp(self):
        super(TestQpidDirectConsumerPublisher, self).setUp()

        self.qpid_opts = [
            cfg.BoolOpt('amqp_durable_queues',
                        default=False,
                        deprecated_name='rabbit_durable_queues',
                        deprecated_group='DEFAULT',
                        help='Use durable queues in amqp.'),
            cfg.BoolOpt('amqp_auto_delete',
                        default=False,
                        help='Auto-delete queues in amqp.'),
            cfg.IntOpt('qpid_topology_version',
                       default=self.qpid_topology,
                       help='qpid topology version'),
        ]

        self.qpid_conf = cfg.ConfigOpts()
        self.qpid_conf.register_opts(self.qpid_opts)
        self.fake_qpid = not _is_qpidd_service_running()

        if self.fake_qpid:
            self.session_receive = get_fake_qpid_session()
            self.session_send = get_fake_qpid_session()
        else:
            self.broker = QPID_BROKER
            # create connection from the qpid.messaging
            # connection for the Consumer.
            self.con_receive = qpid.messaging.Connection(self.broker)
            self.con_receive.open()
            # session to receive the messages
            self.session_receive = self.con_receive.session()

            # connection for sending the message
            self.con_send = qpid.messaging.Connection(self.broker)
            self.con_send.open()
            # session to send the messages
            self.session_send = self.con_send.session()

        # list to store the expected messages and
        # the actual received messages
        self._expected = []
        self._messages = []

    def tearDown(self):
        self.qpid_conf.unregister_opts(self.qpid_opts)
        super(TestQpidDirectConsumerPublisher, self).tearDown()

        if self.fake_qpid:
            _fake_session.flush_exchanges()
        else:
            self.con_receive.close()
            self.con_send.close()

    def consumer_callback(self, msg):
        # This function will be called by the DirectConsumer
        # when any message is received.
        # Append the received message into the messages list
        # so that the received messages can be validated
        # with the expected messages
        if isinstance(msg, dict):
            self._messages.append(msg['content'])
        else:
            self._messages.append(msg)

    def test_qpid_direct_consumer_producer(self):
        self.msgid = str(random.randint(1, 100))

        # create a DirectConsumer and DirectPublisher class objects
        self.dir_cons = qpid_driver.DirectConsumer(self.qpid_conf,
                                                   self.session_receive,
                                                   self.msgid,
                                                   self.consumer_callback)
        self.dir_pub = qpid_driver.DirectPublisher(self.qpid_conf,
                                                   self.session_send,
                                                   self.msgid)

        def try_send_msg(no_msgs):
            for i in range(no_msgs):
                self._expected.append(str(i))
                snd_msg = {'content_type': 'text/plain', 'content': str(i)}
                self.dir_pub.send(snd_msg)

        def try_receive_msg(no_msgs):
            for i in range(no_msgs):
                self.dir_cons.consume()

        thread1 = threading.Thread(target=try_receive_msg,
                                   args=(self.no_msgs,))
        thread2 = threading.Thread(target=try_send_msg,
                                   args=(self.no_msgs,))

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        self.assertEqual(len(self._messages), self.no_msgs)
        self.assertEqual(self._messages, self._expected)


TestQpidDirectConsumerPublisher.generate_scenarios()


class TestQpidTopicAndFanout(test_utils.BaseTestCase):
    """Unit Test cases to test TopicConsumer and
    TopicPublisher classes of the qpid driver
    and FanoutConsumer and FanoutPublisher classes
    of the qpid driver
    """

    _n_qpid_topology = [
        ('v1', dict(qpid_topology=1)),
        ('v2', dict(qpid_topology=2)),
    ]

    _n_msgs = [
        ('single', dict(no_msgs=1)),
        ('multiple', dict(no_msgs=10)),
    ]

    _n_senders = [
        ('single', dict(no_senders=1)),
        ('multiple', dict(no_senders=10)),
    ]

    _n_receivers = [
        ('single', dict(no_receivers=1)),
    ]
    _exchange_class = [
        ('topic', dict(consumer_cls=qpid_driver.TopicConsumer,
                       publisher_cls=qpid_driver.TopicPublisher,
                       topic='topictest.test',
                       receive_topic='topictest.test')),
        ('fanout', dict(consumer_cls=qpid_driver.FanoutConsumer,
                        publisher_cls=qpid_driver.FanoutPublisher,
                        topic='fanouttest',
                        receive_topic='fanouttest')),
    ]

    @classmethod
    def generate_scenarios(cls):
        cls.scenarios = testscenarios.multiply_scenarios(cls._n_qpid_topology,
                                                         cls._n_msgs,
                                                         cls._n_senders,
                                                         cls._n_receivers,
                                                         cls._exchange_class)

    def setUp(self):
        self.qpid_opts = [
            cfg.BoolOpt('amqp_durable_queues',
                        default=False,
                        deprecated_name='rabbit_durable_queues',
                        deprecated_group='DEFAULT',
                        help='Use durable queues in amqp.'),
            cfg.BoolOpt('amqp_auto_delete',
                        default=False,
                        help='Auto-delete queues in amqp.'),
            cfg.IntOpt('qpid_topology_version',
                       default=self.qpid_topology,
                       help='qpid topology version'),
            cfg.StrOpt('control_exchange',
                       default='openstack',
                       help='AMQP exchange to connect to if using Qpid'),
        ]

        super(TestQpidTopicAndFanout, self).setUp()
        self.qpid_conf = cfg.ConfigOpts()
        self.qpid_conf.register_opts(self.qpid_opts)

        self._fake_qpid = not _is_qpidd_service_running()

        if self._fake_qpid:
            self.session_receive = get_fake_qpid_session()
            self.session_send = get_fake_qpid_session()
        else:
            self.broker = QPID_BROKER
            # connection for the Consumer.
            self.con_receive = qpid.messaging.Connection(self.broker)
            self.con_receive.open()
            # session to receive the messages
            self.session_receive = self.con_receive.session()

            # connection for sending the message
            self.con_send = qpid.messaging.Connection(self.broker)
            self.con_send.open()
            # session to send the messages
            self.session_send = self.con_send.session()

        # to store the expected messages and the
        # actual received messages
        self._expected = {}
        self._messages = {}

        self._senders = []
        self._receivers = []

        self._sender_threads = []
        self._receiver_threads = []

    def tearDown(self):
        self.qpid_conf.unregister_opts(self.qpid_opts)
        super(TestQpidTopicAndFanout, self).tearDown()
        if self._fake_qpid:
            _fake_session.flush_exchanges()
        else:
            self.con_receive.close()
            self.con_send.close()

    def consumer_callback(self, msg):
        """callback function called by the ConsumerBase class of
        qpid driver.
        Message will be received in the format x-y
        where x is the sender id and y is the msg number of the sender
        extract the sender id 'x' and store the msg 'x-y' with 'x' as
        the key
        """

        if isinstance(msg, dict):
            msgcontent = msg['content']
        else:
            msgcontent = msg

        splitmsg = msgcontent.split('-')
        key = thread.get_ident()

        if key not in self._messages:
            self._messages[key] = dict()

        tdict = self._messages[key]

        if splitmsg[0] not in tdict:
            tdict[splitmsg[0]] = []

        tdict[splitmsg[0]].append(msgcontent)

    def _try_send_msg(self, sender_id, no_msgs):
        for i in range(no_msgs):
            sendmsg = '%s-%s' % (str(sender_id), str(i))
            key = str(sender_id)
            # Store the message in the self._expected for each sender.
            # This will be used later to
            # validate the test by comparing it with the
            # received messages by all the receivers
            if key not in self._expected:
                self._expected[key] = []
            self._expected[key].append(sendmsg)
            send_dict = {'content_type': 'text/plain', 'content': sendmsg}
            self._senders[sender_id].send(send_dict)

    def _try_receive_msg(self, receiver_id, no_msgs):
        for i in range(self.no_senders * no_msgs):
            no_of_attempts = 0

            # ConsumerBase.consume blocks indefinitely until a message
            # is received.
            # So qpid_receiver.available() is called before calling
            # ConsumerBase.consume() so that we are not
            # blocked indefinitely
            qpid_receiver = self._receivers[receiver_id].get_receiver()
            while no_of_attempts < 50:
                if qpid_receiver.available() > 0:
                    self._receivers[receiver_id].consume()
                    break
                no_of_attempts += 1
                time.sleep(0.05)

    def test_qpid_topic_and_fanout(self):
        for receiver_id in range(self.no_receivers):
            consumer = self.consumer_cls(self.qpid_conf,
                                         self.session_receive,
                                         self.receive_topic,
                                         self.consumer_callback)
            self._receivers.append(consumer)

            # create receivers threads
            thread = threading.Thread(target=self._try_receive_msg,
                                      args=(receiver_id, self.no_msgs,))
            self._receiver_threads.append(thread)

        for sender_id in range(self.no_senders):
            publisher = self.publisher_cls(self.qpid_conf,
                                           self.session_send,
                                           self.topic)
            self._senders.append(publisher)

            # create sender threads
            thread = threading.Thread(target=self._try_send_msg,
                                      args=(sender_id, self.no_msgs,))
            self._sender_threads.append(thread)

        for thread in self._receiver_threads:
                thread.start()

        for thread in self._sender_threads:
                thread.start()

        for thread in self._receiver_threads:
                thread.join()

        for thread in self._sender_threads:
                thread.join()

        # Each receiver should receive all the messages sent by
        # the sender(s).
        # So, Iterate through each of the receiver items in
        # self._messages and compare with the expected messages
        # messages.

        self.assertEqual(len(self._expected), self.no_senders)
        self.assertEqual(len(self._messages), self.no_receivers)

        for key, messages in self._messages.iteritems():
            self.assertEqual(self._expected, messages)

TestQpidTopicAndFanout.generate_scenarios()


def synchronized(func):
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


class FakeQpidMsgManager(object):
    def __init__(self):
        self._exchanges = {}

    @synchronized
    def add_exchange(self, exchange):
        if exchange not in self._exchanges:
            self._exchanges[exchange] = {'msgs': [], 'consumers': {}}

    @synchronized
    def add_exchange_consumer(self, exchange, consumer_id):
        exchange_info = self._exchanges[exchange]
        cons_dict = exchange_info['consumers']
        cons_dict[consumer_id] = 0

    @synchronized
    def add_exchange_msg(self, exchange, msg):
        exchange_info = self._exchanges[exchange]
        exchange_info['msgs'].append(msg)

    def get_exchange_msg(self, exchange, index):
        exchange_info = self._exchanges[exchange]
        return exchange_info['msgs'][index]

    def get_no_exch_msgs(self, exchange):
        exchange_info = self._exchanges[exchange]
        return len(exchange_info['msgs'])

    def get_exch_cons_index(self, exchange, consumer_id):
        exchange_info = self._exchanges[exchange]
        cons_dict = exchange_info['consumers']
        return cons_dict[consumer_id]

    @synchronized
    def inc_consumer_index(self, exchange, consumer_id):
        exchange_info = self._exchanges[exchange]
        cons_dict = exchange_info['consumers']
        cons_dict[consumer_id] += 1

_fake_qpid_msg_manager = FakeQpidMsgManager()


class FakeQpidSessionSender(object):
    def __init__(self, session, id, target, options):
        self.session = session
        self.id = id
        self.target = target
        self.options = options

    @synchronized
    def send(self, object, sync=True, timeout=None):
        _fake_qpid_msg_manager.add_exchange_msg(self.target, object)

    def close(self, timeout=None):
        pass


class FakeQpidSessionReceiver(object):

    def __init__(self, session, id, source, options):
        self.session = session
        self.id = id
        self.source = source
        self.options = options

    @synchronized
    def fetch(self, timeout=None):
        if timeout is None:
            # if timeout is not given, take a default time out
            # of 30 seconds to avoid indefinite loop
            _timeout = 30
        else:
            _timeout = timeout

        deadline = time.time() + _timeout
        while time.time() <= deadline:
            index = _fake_qpid_msg_manager.get_exch_cons_index(self.source,
                                                               self.id)
            try:
                msg = _fake_qpid_msg_manager.get_exchange_msg(self.source,
                                                              index)
            except IndexError:
                pass
            else:
                _fake_qpid_msg_manager.inc_consumer_index(self.source,
                                                          self.id)
                return qpid.messaging.Message(msg)
            time.sleep(0.050)

        if timeout is None:
            raise Exception('timed out waiting for reply')

    def close(self, timeout=None):
        pass

    @synchronized
    def available(self):
        no_msgs = _fake_qpid_msg_manager.get_no_exch_msgs(self.source)
        index = _fake_qpid_msg_manager.get_exch_cons_index(self.source,
                                                           self.id)
        if no_msgs == 0 or index >= no_msgs:
            return 0
        else:
            return no_msgs - index


class FakeQpidSession(object):

    def __init__(self, connection=None, name=None, transactional=None):
        self.connection = connection
        self.name = name
        self.transactional = transactional
        self._receivers = {}
        self.conf = None
        self.url = None
        self._senders = {}
        self._sender_id = 0
        self._receiver_id = 0

    @synchronized
    def sender(self, target, **options):
        exchange_key = self._extract_exchange_key(target)
        _fake_qpid_msg_manager.add_exchange(exchange_key)

        sendobj = FakeQpidSessionSender(self, self._sender_id,
                                        exchange_key, options)
        self._senders[self._sender_id] = sendobj
        self._sender_id = self._sender_id + 1
        return sendobj

    @synchronized
    def receiver(self, source, **options):
        exchange_key = self._extract_exchange_key(source)
        _fake_qpid_msg_manager.add_exchange(exchange_key)
        recvobj = FakeQpidSessionReceiver(self, self._receiver_id,
                                          exchange_key, options)
        self._receivers[self._receiver_id] = recvobj
        _fake_qpid_msg_manager.add_exchange_consumer(exchange_key,
                                                     self._receiver_id)
        self._receiver_id += 1
        return recvobj

    def acknowledge(self, message=None, disposition=None, sync=True):
        pass

    @synchronized
    def flush_exchanges(self):
        _fake_qpid_msg_manager._exchanges = {}

    def _extract_exchange_key(self, exchange_msg):
        """This function extracts a unique key for the exchange.
        This key is used in the dictionary as a 'key' for
        this exchange.
        Eg. if the exchange_msg (for qpid topology version 1)
        is 33/33 ; {"node": {"x-declare": {"auto-delete": true, ....
        then 33 is returned as the key.
        Eg 2. For topology v2, if the
        exchange_msg is - amq.direct/44 ; {"link": {"x-dec.......
        then 44 is returned
        """
        # first check for ';'
        semicolon_split = exchange_msg.split(';')

        # split the first item of semicolon_split  with '/'
        slash_split = semicolon_split[0].split('/')
        # return the last element of the list as the key
        key = slash_split[-1]
        return key.strip()

    def close(self):
        pass

_fake_session = FakeQpidSession()


def get_fake_qpid_session():
    return _fake_session


class QPidHATestCase(test_utils.BaseTestCase):

    scenarios = [('legacy_config', dict(legacy_config=True)),
                 ('transport_url', dict(legacy_config=False))]

    def setUp(self):
        super(QPidHATestCase, self).setUp()
        self.brokers = ['host1', 'host2', 'host3', 'host4', 'host5']
        self.brokers_count = len(self.brokers)

        self.conf.set_override('qpid_port', 5772)
        if self.legacy_config:
            self.conf.set_override('qpid_hosts', self.brokers)
            self.conf.set_override('qpid_username', None)
            self.conf.set_override('qpid_password', None)
            url = None
        else:
            url = 'rabbit://%s' % ','.join(self.brokers)
        url = messaging.TransportURL.parse(self.conf, url)

        self.info = {'attempt': 0,
                     'fail': False}

        def connection_create(myself, broker):
            # do as little work that is enough to pass connection attempt
            myself.connection = mock.Mock()

            expected_broker = self.brokers[self.info['attempt']
                                           % self.brokers_count]
            self.assertEqual(broker['host'], '%s:5772' % expected_broker)

            self.info['attempt'] += 1
            if self.info['fail']:
                raise qpid.messaging.exceptions.ConnectionError

        # just make sure connection instantiation does not fail with an
        # exception
        self.stubs.Set(qpid_driver.Connection, 'connection_create',
                       connection_create)

        # starting from the first broker in the list
        self.connection = qpid_driver.Connection(self.conf, url)
        self.addCleanup(self.connection.close)

        self.info['fail'] = True

    def test_reconnect_order(self):
        self.assertRaises(messaging.MessagingDisconnected,
                          self.connection.reconnect,
                          retry=len(self.brokers))
        self.assertEqual(self.info['attempt'], len(self.brokers) + 1)

    def test_no_reconnect(self):
        self.assertRaises(messaging.MessagingDisconnected,
                          self.connection.reconnect, retry=None)
        self.assertEqual(self.info['attempt'], 2)

    def test_ensure_connected_no_retry(self):
        mock_callback = mock.Mock(
            side_effect=qpid.messaging.exceptions.ConnectionError)
        self.assertRaises(messaging.MessagingDisconnected,
                          self.connection.ensure, None, mock_callback,
                          retry=None)
        self.assertEqual(self.info['attempt'], 2)
        self.assertEqual(mock_callback.call_count, 1)

    def test_ensure_disconnected_no_retry(self):
        self.connection._disconnect()
        mock_callback = mock.Mock(
            side_effect=qpid.messaging.exceptions.ConnectionError)
        self.assertRaises(messaging.MessagingDisconnected,
                          self.connection.ensure, None, mock_callback,
                          retry=None)
        self.assertEqual(self.info['attempt'], 2)
        self.assertEqual(mock_callback.call_count, 0)

    def test_ensure_connected_one_retry(self):
        mock_callback = mock.Mock(
            side_effect=qpid.messaging.exceptions.ConnectionError)
        self.assertRaises(messaging.MessagingDisconnected,
                          self.connection.ensure, None, mock_callback,
                          retry=1)
        self.assertEqual(self.info['attempt'], 2)
        self.assertEqual(mock_callback.call_count, 1)

    def test_ensure_disconnected_one_retry(self):
        self.connection._disconnect()
        mock_callback = mock.Mock(
            side_effect=qpid.messaging.exceptions.ConnectionError)
        self.assertRaises(messaging.MessagingDisconnected,
                          self.connection.ensure, None, mock_callback,
                          retry=1)
        self.assertEqual(self.info['attempt'], 2)
        self.assertEqual(mock_callback.call_count, 1)
