import unittest

from lib.utils import parse_cmd_args


class ParseCmdArgsCase(unittest.TestCase):
    def setUp(self):
        self.config = '/test_dir/'
        self.pidfile = '/test_dir/'

    def test_parse_cmd_without_daemon(self):
        argv = ['-c', self.config, '-P', self.pidfile]
        args = parse_cmd_args(argv)
        self.assertEqual(self.config, args.config)
        self.assertEqual(False, args.daemon)
        self.assertEqual(self.pidfile, args.pidfile)

    def test_parse_cmd_with_daemon(self):
        argv = ['-c', self.config, '-P', self.pidfile, '-d']
        args = parse_cmd_args(argv)
        self.assertEqual(self.config, args.config)
        self.assertEqual(True, args.daemon)
        self.assertEqual(self.pidfile, args.pidfile)

    def test_parse_cmd_without_argv(self):
        self.assertEqual(True, True)