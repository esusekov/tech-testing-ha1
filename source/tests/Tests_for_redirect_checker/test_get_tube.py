import unittest
import mock

from lib.utils import get_tube


class Queue_fake:
    def tube(self, name):
        return name


class GetTubeCase(unittest.TestCase):
    def test_get_tube(self):
        host = 'host_test'
        port = 8080
        space = 1
        name = 'Vasy'
        queue_mock = mock.Mock(return_value=Queue_fake())
        with mock.patch('tarantool_queue.tarantool_queue.Queue', queue_mock):
            result = get_tube(host, port, space, name)
            self.assertEqual(name, result)
            queue_mock.assert_called_once_with(host=host, port=port, space=space)
