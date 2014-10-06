import unittest
import mock

from lib.utils import spawn_workers


def target_fun(config, parent_pid):
    pass

class Process_fake:
    daemon = True
    def start(self):
        pass


class SpawnWorkersCase(unittest.TestCase):
    def test_spawn_workers(self):
        process_mock = mock.Mock(return_value=Process_fake())
        num = 3
        parent_pid = 1
        config = {
            'test1': 1,
            'test2': 2
        }

        with mock.patch('multiprocessing.Process', process_mock):
            spawn_workers(num=num, target=target_fun, args=(config,), parent_pid=parent_pid)
            self.assertEqual(num, process_mock.call_count)


