#!/usr/bin/env python2.7

import os
import sys
import unittest

source_dir = os.path.join(os.path.dirname(__file__), 'source')
sys.path.insert(0, source_dir)

from tests.test_notification_pusher import NotificationPusherTestCase
from tests.Tests_for_redirect_checker.test_redirect_checker import RedirectCheckerTestCase
from tests.Tests_for_redirect_checker.test_check_network_status import CheckNetworkStatusTestCase
from tests.Tests_for_redirect_checker.test_daemonize import DaemonizeCase
from tests.Tests_for_redirect_checker.test_create_pidfile import CreatePidfileCase
from tests.Tests_for_redirect_checker.test_load_config_from_pyfile import LoadConfigFromPyfileCase
from tests.Tests_for_redirect_checker.test_parse_cmd_args import ParseCmdArgsCase
from tests.Tests_for_redirect_checker.test_spawn_workers import SpawnWorkersCase
from tests.Tests_for_redirect_checker.test_get_tube import GetTubeCase
from tests.Tests_for_redirect_checker.test_init import InitCase
from tests.test_worker import WorkerCase

if __name__ == '__main__':
    suite = unittest.TestSuite((
        unittest.makeSuite(NotificationPusherTestCase),
        unittest.makeSuite(CheckNetworkStatusTestCase),
        unittest.makeSuite(RedirectCheckerTestCase),
        unittest.makeSuite(DaemonizeCase),
        unittest.makeSuite(CreatePidfileCase),
        unittest.makeSuite(LoadConfigFromPyfileCase),
        unittest.makeSuite(ParseCmdArgsCase),
        unittest.makeSuite(SpawnWorkersCase),
        unittest.makeSuite(GetTubeCase),
        unittest.makeSuite(InitCase),
        unittest.makeSuite(WorkerCase),
    ))
    result = unittest.TextTestRunner().run(suite)
    sys.exit(not result.wasSuccessful())
