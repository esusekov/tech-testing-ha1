import unittest
import mock
import os

from redirect_checker import (main, main_loop)


flag = True

def helper():
    global flag
    if (flag):
        flag = False
        return True
    else:
        flag = True
        return False


class RedirectCheckerTestCase(unittest.TestCase):
    def test_main(self):
        argv = ['redirect_checker.py', '-c', '/test/', '-P', '/test/', '-d']

        args = mock.Mock()
        args.daemon = True
        args.pidfile = 'test'
        args.config = 'test'

        config = mock.Mock()
        config.LOGGING = 'LOG'
        config.EXIT_CODE = 'EXIT'

        parse_cmd_args_mock = mock.Mock(return_value=args)
        daemonize_mock = mock.Mock()
        create_pidfile_mock = mock.Mock()
        load_config_from_pyfile_mock = mock.Mock(return_value=config)
        dictConfig_mock = mock.Mock()
        main_loop_mock = mock.Mock()

        with mock.patch('redirect_checker.parse_cmd_args', parse_cmd_args_mock):
            with mock.patch('redirect_checker.daemonize', daemonize_mock):
                with mock.patch('redirect_checker.create_pidfile', create_pidfile_mock):
                    with mock.patch('redirect_checker.load_config_from_pyfile', load_config_from_pyfile_mock):
                        with mock.patch('redirect_checker.dictConfig', dictConfig_mock):
                            with mock.patch('redirect_checker.main_loop', main_loop_mock):
                                self.assertEqual(config.EXIT_CODE, main(argv))
                                parse_cmd_args_mock.assert_called_with(argv[1:])
                                main_loop_mock.assert_called_with(config)

    def test_main_no_daemon(self):
        argv = ['redirect_checker.py', '-c', '/test/', '-P', '/test/', '-d']

        args = mock.Mock()
        args.daemon = False
        args.pidfile = 'test'
        args.config = 'test'

        config = mock.Mock()
        config.LOGGING = 'LOG'
        config.EXIT_CODE = 'EXIT'

        parse_cmd_args_mock = mock.Mock(return_value=args)
        daemonize_mock = mock.Mock()
        create_pidfile_mock = mock.Mock()
        load_config_from_pyfile_mock = mock.Mock(return_value=config)
        dictConfig_mock = mock.Mock()
        main_loop_mock = mock.Mock()

        with mock.patch('redirect_checker.parse_cmd_args', parse_cmd_args_mock):
            with mock.patch('redirect_checker.daemonize', daemonize_mock):
                with mock.patch('redirect_checker.create_pidfile', create_pidfile_mock):
                    with mock.patch('redirect_checker.load_config_from_pyfile', load_config_from_pyfile_mock):
                        with mock.patch('redirect_checker.dictConfig', dictConfig_mock):
                            with mock.patch('redirect_checker.main_loop', main_loop_mock):
                                self.assertEqual(config.EXIT_CODE, main(argv))
                                self.assertFalse(daemonize_mock.called)

    def test_main_no_pidfile(self):
        argv = ['redirect_checker.py', '-c', '/test/', '-P', '/test/', '-d']

        args = mock.Mock()
        args.daemon = True
        args.pidfile = None
        args.config = 'test'

        config = mock.Mock()
        config.LOGGING = 'LOG'
        config.EXIT_CODE = 'EXIT'

        parse_cmd_args_mock = mock.Mock(return_value=args)
        daemonize_mock = mock.Mock()
        create_pidfile_mock = mock.Mock()
        load_config_from_pyfile_mock = mock.Mock(return_value=config)
        dictConfig_mock = mock.Mock()
        main_loop_mock = mock.Mock()

        with mock.patch('redirect_checker.parse_cmd_args', parse_cmd_args_mock):
            with mock.patch('redirect_checker.daemonize', daemonize_mock):
                with mock.patch('redirect_checker.create_pidfile', create_pidfile_mock):
                    with mock.patch('redirect_checker.load_config_from_pyfile', load_config_from_pyfile_mock):
                        with mock.patch('redirect_checker.dictConfig', dictConfig_mock):
                            with mock.patch('redirect_checker.main_loop', main_loop_mock):
                                self.assertEqual(config.EXIT_CODE, main(argv))
                                self.assertFalse(create_pidfile_mock.called)

    def test_main_loop_count_0(self):
        getpid_mock = mock.Mock()
        helper_test_mock = mock.Mock(side_effect=helper)
        check_network_mock = mock.Mock(return_value=True)
        active_children_mock = mock.Mock(return_value=[1, 2])
        spawn_worker_mock = mock.Mock()
        sleep_mock = mock.Mock()

        config = mock.Mock()
        config.SLEEP = 1000
        config.WORKER_POOL_SIZE = 2
        config.CHECK_URL = 'test'
        config.HTTP_TIMEOUT = 1000

        with mock.patch('redirect_checker.os.getpid', getpid_mock):
            with mock.patch('redirect_checker.helper_test', helper_test_mock):
                with mock.patch('redirect_checker.check_network_status', check_network_mock):
                    with mock.patch('redirect_checker.active_children', active_children_mock):
                        with mock.patch('redirect_checker.sleep', sleep_mock):
                            main_loop(config)
                            getpid_mock.assert_called_with()
                            check_network_mock.assert_called_with(config.CHECK_URL, config.HTTP_TIMEOUT)
                            active_children_mock.assert_called_with()
                            sleep_mock.assert_called_once_with(config.SLEEP)
                            self.assertEqual(2, helper_test_mock.call_count)

    def test_main_loop_count_not_0(self):
        getpid_mock = mock.Mock()
        helper_test_mock = mock.Mock(side_effect=helper)
        check_network_mock = mock.Mock(return_value=True)
        active_children_mock = mock.Mock(return_value=[1, 2])
        spawn_worker_mock = mock.Mock()
        sleep_mock = mock.Mock()

        config = mock.Mock()
        config.SLEEP = 1000
        config.WORKER_POOL_SIZE = 3
        config.CHECK_URL = 'test'
        config.HTTP_TIMEOUT = 1000

        with mock.patch('redirect_checker.os.getpid', getpid_mock):
            with mock.patch('redirect_checker.helper_test', helper_test_mock):
                with mock.patch('redirect_checker.check_network_status', check_network_mock):
                    with mock.patch('redirect_checker.active_children', active_children_mock):
                        with mock.patch('redirect_checker.spawn_workers', spawn_worker_mock):
                            with mock.patch('redirect_checker.sleep', sleep_mock):
                                main_loop(config)
                                getpid_mock.assert_called_with()
                                check_network_mock.assert_called_with(config.CHECK_URL, config.HTTP_TIMEOUT)
                                active_children_mock.assert_called_with()
                                sleep_mock.assert_called_once_with(config.SLEEP)
                                self.assertEqual(2, helper_test_mock.call_count)
                                self.assertEqual(1, spawn_worker_mock.call_count)

    def test_main_loop_count_check_false(self):

        child = mock.Mock()
        child.terminate.return_value = True

        getpid_mock = mock.Mock()
        helper_test_mock = mock.Mock(side_effect=helper)
        check_network_mock = mock.Mock(return_value=False)
        active_children_mock = mock.Mock(return_value=[child])
        spawn_worker_mock = mock.Mock()
        sleep_mock = mock.Mock()

        config = mock.Mock()
        config.SLEEP = 1000
        config.WORKER_POOL_SIZE = 3
        config.CHECK_URL = 'test'
        config.HTTP_TIMEOUT = 1000

        with mock.patch('redirect_checker.os.getpid', getpid_mock):
            with mock.patch('redirect_checker.helper_test', helper_test_mock):
                with mock.patch('redirect_checker.check_network_status', check_network_mock):
                    with mock.patch('redirect_checker.active_children', active_children_mock):
                        with mock.patch('redirect_checker.sleep', sleep_mock):
                            main_loop(config)
                            getpid_mock.assert_called_with()
                            check_network_mock.assert_called_with(config.CHECK_URL, config.HTTP_TIMEOUT)
                            sleep_mock.assert_called_once_with(config.SLEEP)
                            self.assertEqual(2, helper_test_mock.call_count)
                            active_children_mock.assert_called_with()
                            self.assertEqual(1, child.terminate.call_count)









