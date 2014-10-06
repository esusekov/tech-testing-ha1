import unittest
import mock
import socket

from lib.utils import check_network_status


class CheckNetworkStatusTestCase(unittest.TestCase):
    def setUp(self):
        self.url = 'http://test.com'
        self.time = 1
        self.urlopen_mock = mock.Mock()

    def test_positive_execution(self):
        with mock.patch('urllib2.urlopen', self.urlopen_mock):
            self.assertTrue(check_network_status(self.url, self.time))
            self.urlopen_mock.assert_called_with(url=self.url, timeout=self.time)

    def test_negative_execution(self):
        with mock.patch('urllib2.urlopen', self.urlopen_mock):
            self.urlopen_mock.side_effect = socket.error
            self.assertFalse(check_network_status(self.url, self.time))
            self.urlopen_mock.assert_called_with(url=self.url, timeout=self.time)