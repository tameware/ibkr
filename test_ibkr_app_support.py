# Usage: python -m unittest test_ibkr_app_support.py -v

import datetime
import unittest
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from ibkr_app_support import (
    log_session_transition,
    regular_session_open,
    session_wall_clock,
    wait_for_ib_ready,
)

_SESSION_CFG = {
    "market_timezone": "America/New_York",
    "market_open_hour": 9,
    "market_open_minute": 30,
    "market_close_hour": 16,
}


class TestRegularSession(unittest.TestCase):
    def setUp(self):
        self.ny = ZoneInfo("America/New_York")

    def test_regular_session_open_during_hours(self):
        now = datetime.datetime(2026, 5, 11, 10, 30, 0, tzinfo=self.ny)
        self.assertTrue(regular_session_open(_SESSION_CFG, now=now))

    def test_regular_session_open_before_open(self):
        now = datetime.datetime(2026, 5, 11, 9, 29, 0, tzinfo=self.ny)
        self.assertFalse(regular_session_open(_SESSION_CFG, now=now))

    def test_regular_session_open_at_open(self):
        now = datetime.datetime(2026, 5, 11, 9, 30, 0, tzinfo=self.ny)
        self.assertTrue(regular_session_open(_SESSION_CFG, now=now))

    def test_regular_session_open_after_close(self):
        now = datetime.datetime(2026, 5, 11, 16, 0, 0, tzinfo=self.ny)
        self.assertFalse(regular_session_open(_SESSION_CFG, now=now))

    def test_session_wall_clock_naive_localizes(self):
        naive = datetime.datetime(2026, 5, 11, 10, 0, 0)
        now, tz_name, moh, mom, mch = session_wall_clock(_SESSION_CFG, now=naive)
        self.assertEqual(tz_name, "America/New_York")
        self.assertEqual(moh, 9)
        self.assertEqual(mom, 30)
        self.assertEqual(mch, 16)
        self.assertIsNotNone(now.tzinfo)

    def test_log_session_transition_startup_closed(self):
        logger = MagicMock()
        log_session_transition(
            logger,
            _SESSION_CFG,
            prev_in_hours=None,
            in_hours=False,
        )
        logger.info.assert_called_once()
        self.assertIn("closed at startup", logger.info.call_args[0][0])

    def test_log_session_transition_session_end(self):
        logger = MagicMock()
        log_session_transition(
            logger,
            _SESSION_CFG,
            prev_in_hours=True,
            in_hours=False,
        )
        logger.info.assert_called_once()
        self.assertIn("session ended", logger.info.call_args[0][0])

    def test_log_session_transition_no_op_when_open(self):
        logger = MagicMock()
        log_session_transition(
            logger,
            _SESSION_CFG,
            prev_in_hours=True,
            in_hours=True,
        )
        logger.info.assert_not_called()


class TestWaitForIbReady(unittest.TestCase):
    def test_returns_true_when_ready_immediately(self):
        self.assertTrue(wait_for_ib_ready(lambda: True, timeout_seconds=1.0))

    def test_returns_false_on_timeout(self):
        self.assertFalse(
            wait_for_ib_ready(lambda: False, timeout_seconds=0.2, poll_seconds=0.05)
        )

    def test_returns_false_if_connection_drops_after_connect(self):
        state = {"up": True}

        def connected():
            return state["up"]

        def ready():
            return False

        def drop():
            state["up"] = False

        import threading

        timer = threading.Timer(0.05, drop)
        timer.start()
        self.addCleanup(timer.cancel)
        self.assertFalse(
            wait_for_ib_ready(
                ready,
                is_connected=connected,
                timeout_seconds=1.0,
                poll_seconds=0.02,
            )
        )


if __name__ == "__main__":
    unittest.main()
