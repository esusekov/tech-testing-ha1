import unittest
import mock
from mock import patch, Mock
from tarantool.error import DatabaseError

import lib.worker as wr


class WorkerCase(unittest.TestCase):
    @patch('lib.worker.to_unicode')
    @patch('lib.worker.get_redirect_history')
    def test_get_redirect_history_from_task_when_error_in_history_types(self, get_r_history_m, to_uni_m):
        task_mock = Mock(None)
        task_mock.data = {
            'recheck': False,
            'url': 'www.leningrad.spb.ru',
            'url_id': 666
        }
        test_history_types = ['ERROR', 'APPLE']
        get_r_history_m.return_value = test_history_types, Mock(None), Mock(None)
        url_mock = Mock(None)
        to_uni_m.return_value = url_mock

        res_is_input, res_data = wr.get_redirect_history_from_task(task_mock, 42)

        to_uni_m.assert_called_once_with('www.leningrad.spb.ru', 'ignore')
        self.assertTrue(res_is_input)
        test_data = {
            'recheck': True,
            'url': 'www.leningrad.spb.ru',
            'url_id': 666
        }
        self.assertEqual(res_data, test_data)

    @patch('lib.worker.to_unicode')
    @patch('lib.worker.get_redirect_history')
    def test_get_redirect_history_from_task_when_suspicious_in_data(self, get_r_history_m, to_uni_m):
        task_mock = Mock(None)
        task_mock.data = {
            'recheck': False,
            'url': 'www.leningrad.spb.ru',
            'url_id': 666,
            'suspicious': 'whazzzup'
        }
        test_history_types = ['APPLE', 'BLACKBERRY']
        test_history_urls = ['apple.com', 'blackberry.com']
        test_counters = ['a', 'b']
        get_r_history_m.return_value = test_history_types,  test_history_urls, test_counters
        url_mock = Mock(None)
        to_uni_m.return_value = url_mock

        res_is_input, res_data = wr.get_redirect_history_from_task(task_mock, 42)

        self.assertFalse(res_is_input)
        test_data = {
            'url_id': 666,
            'result': [test_history_types, test_history_urls, test_counters],
            'check_type': 'normal',
            'suspicious': 'whazzzup'
        }
        self.assertEqual(res_data, test_data)

    @patch('lib.worker.to_unicode')
    @patch('lib.worker.get_redirect_history')
    def test_get_redirect_history_from_task_when_everything_ok(self, get_r_history_m, to_uni_m):
        task_mock = Mock(None)
        task_mock.data = {
            'recheck': False,
            'url': 'www.leningrad.spb.ru',
            'url_id': 666
        }
        test_history_types = ['APPLE', 'BLACKBERRY']
        test_history_urls = ['apple.com', 'blackberry.com']
        test_counters = ['a', 'b']
        get_r_history_m.return_value = test_history_types,  test_history_urls, test_counters
        url_mock = Mock(None)
        to_uni_m.return_value = url_mock

        res_is_input, res_data = wr.get_redirect_history_from_task(task_mock, 42)

        self.assertFalse(res_is_input)
        test_data = {
            'url_id': 666,
            'result': [test_history_types, test_history_urls, test_counters],
            'check_type': 'normal'
        }
        self.assertEqual(res_data, test_data)

    @patch('lib.worker.get_tube')
    def test_get_tubes(self, get_tube_m):
        config = Mock(None)
        config.INPUT_QUEUE_HOST = 'fake_in.com'
        config.INPUT_QUEUE_PORT = '6666'
        config.INPUT_QUEUE_SPACE = 10
        config.INPUT_QUEUE_TUBE = 'fake_tube_in'
        config.OUTPUT_QUEUE_HOST = 'fake_out.com'
        config.OUTPUT_QUEUE_PORT = '9999'
        config.OUTPUT_QUEUE_SPACE = 20
        config.OUTPUT_QUEUE_TUBE = 'fake_tube_out'
        in_tube_mock = Mock(None)
        out_tube_mock = Mock(None)
        in_tube_mock.opt = {'tube': 'test'}
        out_tube_mock.opt = {'tube': 'test'}
        get_tube_m.side_effect = [in_tube_mock, out_tube_mock]
        res_in_tube, res_out_tube = wr.get_tubes(config)

        self.assertEqual(get_tube_m.call_count, 2)
        self.assertEqual(res_in_tube, in_tube_mock)
        self.assertEqual(res_out_tube, out_tube_mock)

    @patch('lib.worker.get_redirect_history_from_task')
    @patch('lib.worker.get_tubes')
    @patch('os.path.exists')
    def test_worker_when_no_result_and_no_exception(self, path_exs_m, get_tubes_m, get_redirect_m):
        config = Mock(None)
        config.HTTP_TIMEOUT = 100
        config.MAX_REDIRECTS = 10
        config.USER_AGENT = 'abc'
        config.QUEUE_TAKE_TIMEOUT = 50

        task_mock = Mock(None)
        task_mock.ack = Mock(None)
        in_tube_mock = Mock(None)
        out_tube_mock = Mock(None)
        in_tube_mock.take = Mock(return_value=task_mock)
        in_tube_mock.put = Mock(None)
        out_tube_mock.put = Mock(None)
        path_exs_m.side_effect = [True, False]
        get_redirect_m.return_value = None
        get_tubes_m.return_value = in_tube_mock, out_tube_mock

        wr.worker(config, 666)

        get_tubes_m.assert_called_once_with(config)
        in_tube_mock.take.assert_called_once_with(50)
        get_redirect_m.assert_called_once_with(task_mock, 100, 10, 'abc')
        self.assertEqual(in_tube_mock.put.call_count, 0)
        self.assertEqual(out_tube_mock.put.call_count, 0)
        self.assertTrue(task_mock.ack.called)

    @patch('lib.worker.get_redirect_history_from_task')
    @patch('lib.worker.get_tubes')
    @patch('os.path.exists')
    @patch('lib.worker.logger.exception')
    def test_worker_when_no_result_and_exception(self, exc_m, path_exs_m, get_tubes_m, get_redirect_m):
        config = Mock(None)
        config.HTTP_TIMEOUT = 100
        config.MAX_REDIRECTS = 10
        config.USER_AGENT = 'abc'
        config.QUEUE_TAKE_TIMEOUT = 50

        task_mock = Mock(None)
        task_mock.ack = Mock(side_effect=DatabaseError())
        in_tube_mock = Mock(None)
        out_tube_mock = Mock(None)
        in_tube_mock.take = Mock(return_value=task_mock)
        in_tube_mock.put = Mock(None)
        out_tube_mock.put = Mock(None)
        path_exs_m.side_effect = [True, False]
        get_redirect_m.return_value = None
        get_tubes_m.return_value = in_tube_mock, out_tube_mock

        wr.worker(config, 666)

        self.assertTrue(exc_m.called)

    @patch('lib.worker.get_redirect_history_from_task')
    @patch('lib.worker.get_tubes')
    @patch('os.path.exists')
    def test_worker_when_result_putting_to_input(self, path_exs_m, get_tubes_m, get_redirect_m):
        config = Mock(None)
        config.HTTP_TIMEOUT = 100
        config.MAX_REDIRECTS = 10
        config.USER_AGENT = 'abc'
        config.QUEUE_TAKE_TIMEOUT = 50
        config.RECHECK_DELAY = 1000

        task_mock = Mock(None)
        task_mock.ack = Mock(None)
        task_mock.meta = Mock(return_value={'pri': 'test'})
        in_tube_mock = Mock(None)
        out_tube_mock = Mock(None)
        in_tube_mock.take = Mock(return_value=task_mock)
        in_tube_mock.put = Mock(None)
        out_tube_mock.put = Mock(None)
        path_exs_m.side_effect = [True, False]
        get_redirect_m.return_value = True, 'data'
        get_tubes_m.return_value = in_tube_mock, out_tube_mock

        wr.worker(config, 666)

        in_tube_mock.put.assert_called_once_with('data', delay=1000, pri='test')
        self.assertFalse(out_tube_mock.put.called)

    @patch('lib.worker.get_redirect_history_from_task')
    @patch('lib.worker.get_tubes')
    @patch('os.path.exists')
    def test_worker_when_result_putting_to_output(self, path_exs_m, get_tubes_m, get_redirect_m):
        config = Mock(None)
        config.HTTP_TIMEOUT = 100
        config.MAX_REDIRECTS = 10
        config.USER_AGENT = 'abc'
        config.QUEUE_TAKE_TIMEOUT = 50

        task_mock = Mock(None)
        task_mock.ack = Mock(None)
        in_tube_mock = Mock(None)
        out_tube_mock = Mock(None)
        in_tube_mock.take = Mock(return_value=task_mock)
        in_tube_mock.put = Mock(None)
        out_tube_mock.put = Mock(None)
        path_exs_m.side_effect = [True, False]
        get_redirect_m.return_value = False, 'data'
        get_tubes_m.return_value = in_tube_mock, out_tube_mock

        wr.worker(config, 666)

        out_tube_mock.put.assert_called_once_with('data')
        self.assertFalse(in_tube_mock.put.called)

    @patch('lib.worker.get_redirect_history_from_task')
    @patch('lib.worker.get_tubes')
    @patch('os.path.exists')
    def test_worker_when_no_task_at_all(self, path_exs_m, get_tubes_m, get_redirect_m):
        config = Mock(None)
        config.QUEUE_TAKE_TIMEOUT = 50

        in_tube_mock = Mock(None)
        out_tube_mock = Mock(None)
        in_tube_mock.take = Mock(return_value=None)
        in_tube_mock.put = Mock(None)
        out_tube_mock.put = Mock(None)
        path_exs_m.side_effect = [True, False]
        get_tubes_m.return_value = in_tube_mock, out_tube_mock

        wr.worker(config, 666)

        self.assertFalse(get_redirect_m.called)