import unittest
import mock

from lib.utils import load_config_from_pyfile


config_test = {
        'SLEEP': 10,
        'HTTP_TIMEOUT': 3,
        'MAX_REDIRECTS': 30,
        'not_valid': 'test'
}

def execfile_fake(filepath, variables):
    variables.update(config_test)


class LoadConfigFromPyfileCase(unittest.TestCase):
    def test_load_config(self):
        filepath = '/test/'
        with mock.patch('__builtin__.execfile', side_effect=execfile_fake):
            config = load_config_from_pyfile(filepath)
            self.assertEqual(config_test['SLEEP'], config.SLEEP)
            self.assertEqual(config_test['HTTP_TIMEOUT'], config.HTTP_TIMEOUT)
            self.assertEqual(config_test['MAX_REDIRECTS'], config.MAX_REDIRECTS)
            self.assertFalse(hasattr(config, 'not_valid'))
