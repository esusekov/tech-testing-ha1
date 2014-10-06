import unittest
import mock

from lib.utils import daemonize

flag = True


def fork_fake():
    global flag
    if flag:
        flag = False
        return 0
    else:
        return 1

class DaemonizeCase(unittest.TestCase):
    def setUp(self):
        self.exit_mock = mock.Mock()

    def test_daemonize_pid_not_null(self):
        pid = 1
        with mock.patch('os.fork', mock.Mock(return_value=pid)):
            with mock.patch('os._exit', self.exit_mock):
                daemonize()
                self.exit_mock.assert_called_once_with(0)

    def test_daemonize_pid_null(self):
        pid = 0
        with mock.patch('os.fork', mock.Mock(return_value=pid)):
            with mock.patch('os.setsid', mock.Mock()):
                with mock.patch('os._exit', self.exit_mock):
                    daemonize()
                    self.assertEqual(0, self.exit_mock.call_count)

    def test_daemonize_pid_null_one(self):
        with mock.patch('os.fork', side_effect=fork_fake):
            with mock.patch('os.setsid', mock.Mock()):
                with mock.patch('os._exit', self.exit_mock):
                    daemonize()
                    self.assertEqual(1, self.exit_mock.call_count)
                    self.exit_mock.assert_called_once_with(0)

