import unittest
import argparse
import mock
from mock import patch, Mock
import notification_pusher as np
from Queue import Queue

import tarantool
import requests
from gevent import queue as gevent_queue


def stop_app(first):
    np.run_application = False


class Task:
    def __init__(self, _id, _data):
        self.data = _data
        self.task_id = _id


class NotificationPusherTestCase(unittest.TestCase):
    def test_create_pidfile(self):
        pid = 42
        m_open = mock.mock_open()
        with mock.patch('notification_pusher.open', m_open, create=True):
            with mock.patch('os.getpid', mock.Mock(return_value=pid)):
                np.create_pidfile('/file/path')

        m_open.assert_called_once_with('/file/path', 'w')
        m_open().write.assert_called_once_with(str(pid))

    def test_notification_worker(self):
        q = Queue()
        task1 = Task(1, {'callback_url': 'fakeurl1.com'})
        task2 = Task(2, {'callback_url': 'fakeurl2.com'})

        with patch('requests.post', Mock(None)):
            with patch('notification_pusher.logger.info', Mock(return_value=Mock())):
                np.notification_worker(task1, q)
                np.notification_worker(task2, q)

        self.assertEqual(q.qsize(), 2)
        q.get()
        resulting_task = q.get()
        self.assertEqual(resulting_task[0].data, task2.data)
        self.assertEqual(resulting_task[1], 'ack')

    def test_notification_worker_with_exc(self):
        q = Queue()
        task3 = Task(3, {'callback_url': 'fakeurl3.com'})

        with patch('requests.post', Mock(side_effect=requests.RequestException())):
            with patch('notification_pusher.logger.exception', Mock(None)):
                np.notification_worker(task3, q)

        resulting_task = q.get()
        self.assertEqual(resulting_task[1], 'bury')

    def test_done_with_processed_tasks(self):
        q = Queue()
        task1 = mock.Mock()
        task2 = mock.Mock()
        q.put((task1, 'ack'))
        q.put((task2, 'bury'))

        np.done_with_processed_tasks(q)

        self.assertTrue(task1.ack.called)
        self.assertTrue(task2.bury.called)
        self.assertEqual(q.qsize(), 0)

    def test_done_with_processed_tasks_with_exc(self):
        q = Queue()
        task1 = mock.Mock()
        q.put((task1, 'ack'))

        exc_mock = Mock()
        task1.ack = mock.Mock(side_effect=tarantool.DatabaseError())
        with patch('notification_pusher.logger.exception', exc_mock):
            np.done_with_processed_tasks(q)

        self.assertTrue(exc_mock.called)

    def test_done_with_processed_tasks_empty_queue(self):
        q = Mock(None)
        q.qsize = Mock(return_value=42)
        q.get_nowait = Mock(side_effect=gevent_queue.Empty)

        np.done_with_processed_tasks(q)

        self.assertRaises(gevent_queue.Empty)

    @patch('notification_pusher.run_application', True)
    @patch('notification_pusher.exit_code', 0)
    def test_stop_handler(self):
        np.stop_handler(1)
        self.assertFalse(np.run_application)
        self.assertEqual(np.exit_code, 129)

        np.stop_handler(-1)
        self.assertEqual(np.exit_code, 127)

        np.stop_handler(0)
        self.assertEqual(np.exit_code, 128)

    def test_parse_cmd_args(self):
        test_args1 = ['-c', 'test_config', '-d', '-P', 'pidfile']
        result1 = np.parse_cmd_args(test_args1)
        self.assertEqual(result1, argparse.Namespace(config='test_config', daemon=True, pidfile='pidfile'))

        test_args2 = ['-c', 'test_config', '-P', 'pidfile']
        result2 = np.parse_cmd_args(test_args2)
        self.assertEqual(result2, argparse.Namespace(config='test_config', daemon=False, pidfile='pidfile'))

    def test_daemonize_pid_null(self):
        pid = 0
        exit_mock = Mock(None)

        with patch('os.fork', Mock(return_value=pid)):
            with patch('os.setsid', Mock(None)):
                with patch('os._exit', exit_mock):
                    np.daemonize()
                    self.assertEqual(0, exit_mock.call_count)

    def test_daemonize_pid_null_with_exc_in_the_start(self):
        with patch('os.fork', Mock(side_effect=OSError('error'))):
            self.assertRaises(Exception, np.daemonize)

    def test_daemonize_pid_null_with_exc_in_the_end(self):
        setsid_mock = Mock(None)

        with patch('os.fork', Mock(side_effect=[0, OSError('error')])):
            with patch('os.setsid', setsid_mock):
                self.assertRaises(Exception, np.daemonize)
                self.assertTrue(setsid_mock.called)

    def test_daemonize_pid_not_null(self):
        pid = 42
        exit_mock = Mock(None)
        setsid_mock = Mock(None)

        with patch('os.fork', Mock(return_value=pid)):
            with patch('os.setsid', setsid_mock):
                with patch('os._exit', exit_mock):
                    np.daemonize()
                    self.assertEqual(0, setsid_mock.call_count)
                    exit_mock.assert_called_once_with(0)

    def test_daemonize_pid_null_then_not_null(self):
        exit_mock = Mock(None)
        setsid_mock = Mock(None)

        with patch('os.fork', Mock(side_effect=[0, 42])):
            with patch('os.setsid', setsid_mock):
                with patch('os._exit', exit_mock):
                    np.daemonize()
                    self.assertTrue(setsid_mock.called)
                    exit_mock.assert_called_once_with(0)

    def test_load_config(self):
        filepath = '/test/'
        config_test = {'SLEEP': 10, 'HTTP_TIMEOUT': 3, 'MAX_REDIRECTS': 30, 'not_valid': 'test'}

        def execfile_fake(filepath, variables):
            variables.update(config_test)

        with patch('__builtin__.execfile', side_effect=execfile_fake):
            config = np.load_config_from_pyfile(filepath)
            self.assertEqual(config_test['SLEEP'], config.SLEEP)
            self.assertEqual(config_test['HTTP_TIMEOUT'], config.HTTP_TIMEOUT)
            self.assertEqual(config_test['MAX_REDIRECTS'], config.MAX_REDIRECTS)
            self.assertFalse(hasattr(config, 'not_valid'))

    @patch('notification_pusher.signal')
    def test_install_signal_handlers(self, signal_m):
        gsig_mock = Mock(None)
        stop_handler_mock = Mock(None)
        signal_m.SIGTERM = 10
        signal_m.SIGINT = 20
        signal_m.SIGHUP = 30
        signal_m.SIGQUIT = 40

        with patch('gevent.signal', gsig_mock):
            with patch('notification_pusher.stop_handler', stop_handler_mock):
                np.install_signal_handlers()

        gsig_mock.assert_any_call(10, stop_handler_mock, 10)
        gsig_mock.assert_any_call(20, stop_handler_mock, 20)
        gsig_mock.assert_any_call(30, stop_handler_mock, 30)
        gsig_mock.assert_any_call(40, stop_handler_mock, 40)

    def test_configure(self):
        config = Mock(None)
        config.QUEUE_HOST = '1.1.1.1'
        config.QUEUE_PORT = '6666'
        config.QUEUE_SPACE = 30
        config.QUEUE_TUBE = 1
        config.QUEUE_TAKE_TIMEOUT = 10
        config.WORKER_POOL_SIZE = 10
        config.SLEEP = 10

        queue_mock = Mock(None)
        process_queue_mock = Mock(None)
        pool_mock = Mock(None)

        with patch('tarantool_queue.Queue', queue_mock):
            with patch('gevent.queue.Queue', process_queue_mock):
                with patch('notification_pusher.Pool', pool_mock):
                    np.configure(config)

        queue_mock.assert_called_once_with(host=config.QUEUE_HOST, port=config.QUEUE_PORT, space=config.QUEUE_SPACE)
        pool_mock.assert_called_once_with(config.WORKER_POOL_SIZE)
        self.assertTrue(process_queue_mock.called)

    def test_add_worker(self):
        worker_mock = Mock(None)
        greenlet_mock = Mock(return_value=worker_mock)
        config_mock = Mock(None)
        config_mock.HTTP_CONNECTION_TIMEOUT = 1000
        task_mock = Mock(None)
        pool_mock = Mock(None)
        queue_mock = Mock(None)
        notify_mock = Mock(None)

        with patch('notification_pusher.notification_worker', notify_mock):
            with patch('notification_pusher.Greenlet', greenlet_mock):
                np.add_worker(config_mock, task_mock, 42, pool_mock, queue_mock)

        greenlet_mock.assert_called_with(notify_mock, task_mock, queue_mock,
                                         timeout=config_mock.HTTP_CONNECTION_TIMEOUT, verify=False)
        pool_mock.add.assert_called_with(worker_mock)
        self.assertTrue(worker_mock.start.called)

    @patch('notification_pusher.run_application', True)
    @patch('notification_pusher.configure')
    @patch('notification_pusher.done_with_processed_tasks')
    @patch('notification_pusher.add_worker')
    def test_main_loop_when_app_is_running(self, add_work_mock, done_mock, configure_mock):
        config_mock = Mock(None)
        config_mock.QUEUE_TAKE_TIMEOUT = 10
        config_mock.SLEEP = 10
        task_mock = Mock(None)
        tube_mock = Mock(None)
        tube_mock.take = Mock(return_value=task_mock)
        pool_mock = Mock(None)
        pool_mock.free_count = Mock(return_value=5)
        queue_mock = Mock(None)
        configure_mock.return_value = tube_mock, pool_mock, queue_mock
        sleep_mock = Mock(side_effect=stop_app)

        with patch('notification_pusher.sleep', sleep_mock):
            np.main_loop(config_mock)

        configure_mock.assert_called_once_with(config_mock)
        self.assertTrue(pool_mock.free_count.called)
        self.assertEqual(tube_mock.take.call_count, 5)
        tube_mock.take.assert_call_any(config_mock.QUEUE_TAKE_TIMEOUT)
        done_mock.assert_called_once_with(queue_mock)
        sleep_mock.assert_called_once_with(config_mock.SLEEP)

    @patch('notification_pusher.run_application', True)
    @patch('notification_pusher.configure')
    @patch('notification_pusher.done_with_processed_tasks')
    @patch('notification_pusher.add_worker')
    def test_main_loop_when_app_is_running_but_no_tasks(self, add_work_mock, done_mock, configure_mock):
        config_mock = Mock(None)
        config_mock.QUEUE_TAKE_TIMEOUT = 10
        config_mock.SLEEP = 10
        tube_mock = Mock(None)
        tube_mock.take = Mock(return_value=None)
        pool_mock = Mock(None)
        pool_mock.free_count = Mock(return_value=5)
        queue_mock = Mock(None)
        configure_mock.return_value = tube_mock, pool_mock, queue_mock
        sleep_mock = Mock(side_effect=stop_app)

        with patch('notification_pusher.sleep', sleep_mock):
            np.main_loop(config_mock)

        self.assertFalse(add_work_mock.called)

    @patch('notification_pusher.run_application', False)
    @patch('notification_pusher.configure')
    @patch('notification_pusher.logger.info')
    def test_main_loop_when_app_is_not_running(self, info_mock, configure_mock):
        config_mock = Mock(None)
        configure_mock.return_value = Mock(None), Mock(None), Mock(None)

        np.main_loop(config_mock)

        info_mock.assert_called_once_with('Stop application loop.')

    @patch('notification_pusher.run_application', True)
    @patch('notification_pusher.parse_cmd_args')
    @patch('notification_pusher.daemonize')
    @patch('notification_pusher.create_pidfile')
    @patch('notification_pusher.load_config_from_pyfile')
    @patch('notification_pusher.patch_all')
    @patch('notification_pusher.dictConfig')
    @patch('notification_pusher.install_signal_handlers')
    @patch('notification_pusher.main_loop')
    @patch('notification_pusher.sleep')
    def test_main(self, sleep_m, main_m, inst_sig_m, dictConf_m, patch_m, load_conf_m, create_pid_m, daemon_m, parse_cmd_m):
        args_mock = Mock(None)
        args_mock.daemon = True
        args_mock.pidfile = True
        args_mock.config = 'test'
        conf_mock = Mock(None)
        conf_mock.LOGGING = 'test'
        conf_mock.SLEEP_ON_FAIL = 100
        main_m.side_effect = stop_app
        parse_cmd_m.return_value = args_mock
        load_conf_m.return_value = conf_mock

        np.main([])

        self.assertTrue(daemon_m.called)
        create_pid_m.assert_called_once_with(args_mock.pidfile)
        self.assertTrue(load_conf_m.called)
        self.assertTrue(patch_m.called)
        dictConf_m.assert_called_once_with(conf_mock.LOGGING)
        self.assertTrue(inst_sig_m.called)
        main_m.assert_called_once_with(conf_mock)
        self.assertEqual(0, sleep_m.call_count)

    @patch('notification_pusher.run_application', True)
    @patch('notification_pusher.parse_cmd_args')
    @patch('notification_pusher.daemonize')
    @patch('notification_pusher.create_pidfile')
    @patch('notification_pusher.load_config_from_pyfile')
    @patch('notification_pusher.patch_all')
    @patch('notification_pusher.dictConfig')
    @patch('notification_pusher.install_signal_handlers')
    @patch('notification_pusher.main_loop')
    @patch('notification_pusher.sleep')
    @patch('notification_pusher.logger')
    def test_main_with_exc(self, log_m, sleep_m, main_m, inst_sig_m, dictConf_m, patch_m, load_conf_m, create_pid_m, daemon_m, parse_cmd_m):
        args_mock = Mock(None)
        args_mock.daemon = True
        args_mock.pidfile = True
        args_mock.config = 'test'
        conf_mock = Mock(None)
        conf_mock.LOGGING = 'test'
        conf_mock.SLEEP_ON_FAIL = 100
        main_m.side_effect = Exception('err')
        sleep_m.side_effect = stop_app
        parse_cmd_m.return_value = args_mock
        load_conf_m.return_value = conf_mock

        np.main([])

        sleep_m.assert_called_once_with(conf_mock.SLEEP_ON_FAIL)

    @patch('notification_pusher.run_application', True)
    @patch('notification_pusher.parse_cmd_args')
    @patch('notification_pusher.daemonize')
    @patch('notification_pusher.create_pidfile')
    @patch('notification_pusher.load_config_from_pyfile')
    @patch('notification_pusher.patch_all')
    @patch('notification_pusher.dictConfig')
    @patch('notification_pusher.install_signal_handlers')
    @patch('notification_pusher.main_loop')
    @patch('notification_pusher.sleep')
    def test_main_no_daemon(self, sleep_m, main_m, inst_sig_m, dictConf_m, patch_m, load_conf_m, create_pid_m, daemon_m, parse_cmd_m):
        args_mock = Mock(None)
        args_mock.daemon = False
        args_mock.pidfile = True
        args_mock.config = 'test'
        conf_mock = Mock(None)
        conf_mock.LOGGING = 'test'
        conf_mock.SLEEP_ON_FAIL = 100
        main_m.side_effect = stop_app
        parse_cmd_m.return_value = args_mock
        load_conf_m.return_value = conf_mock

        np.main([])

        self.assertFalse(daemon_m.called)

    @patch('notification_pusher.run_application', True)
    @patch('notification_pusher.parse_cmd_args')
    @patch('notification_pusher.daemonize')
    @patch('notification_pusher.create_pidfile')
    @patch('notification_pusher.load_config_from_pyfile')
    @patch('notification_pusher.patch_all')
    @patch('notification_pusher.dictConfig')
    @patch('notification_pusher.install_signal_handlers')
    @patch('notification_pusher.main_loop')
    @patch('notification_pusher.sleep')
    def test_main_no_pidfile(self, sleep_m, main_m, inst_sig_m, dictConf_m, patch_m, load_conf_m, create_pid_m, daemon_m, parse_cmd_m):
        args_mock = Mock(None)
        args_mock.daemon = True
        args_mock.pidfile = False
        args_mock.config = 'test'
        conf_mock = Mock(None)
        conf_mock.LOGGING = 'test'
        conf_mock.SLEEP_ON_FAIL = 100
        main_m.side_effect = stop_app
        parse_cmd_m.return_value = args_mock
        load_conf_m.return_value = conf_mock

        np.main([])

        self.assertFalse(create_pid_m.called)