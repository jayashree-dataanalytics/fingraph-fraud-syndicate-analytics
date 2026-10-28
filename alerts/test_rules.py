"""Unit tests for the alert rules engine (no Neo4j needed).

    python alerts/test_rules.py -v
"""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import rules_engine as eng  # noqa: E402


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def data(self):
        return self.rows


class FakeSession:
    def __init__(self, accounts, groups):
        self.accounts, self.groups = accounts, groups

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def run(self, query, **params):
        rows = self.accounts if query is eng.ACCOUNT_Q else self.groups
        return FakeResult([r for r in rows if r["score"] >= params["t"]])


class FakeDriver:
    def __init__(self, accounts=(), groups=()):
        self.accounts, self.groups = list(accounts), list(groups)

    def session(self):
        return FakeSession(self.accounts, self.groups)


T0 = 1_700_000_000  # a realistic "now" (unix seconds)

ACCOUNT_RULE = {"name": "acct", "type": "account", "threshold": 0.8, "cooldown_minutes": 30}
GROUP_RULE = {"name": "grp", "type": "group", "threshold": 0.7, "min_members": 3, "cooldown_minutes": 60}


class RulesEngineTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        patcher = mock.patch.object(eng, "STATE_FILE", Path(tmp.name) / "state.json")
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_engine(self, driver, rules, now):
        with mock.patch.object(eng, "dispatch") as dispatch, mock.patch.object(eng.time, "time", return_value=now):
            eng.run_once(driver, {"rules": rules}, dry_run=True)
        return dispatch

    def test_only_scores_at_or_above_threshold_alert(self):
        driver = FakeDriver(accounts=[
            {"key": "A1", "score": 1.0, "community": 7},
            {"key": "A2", "score": 0.79, "community": 7},
        ])
        dispatch = self.run_engine(driver, [ACCOUNT_RULE], now=T0)
        self.assertEqual(dispatch.call_count, 1)
        self.assertIn("A1", dispatch.call_args[0][1])

    def test_cooldown_blocks_repeat_alert(self):
        driver = FakeDriver(accounts=[{"key": "A1", "score": 0.9, "community": 7}])
        self.assertEqual(self.run_engine(driver, [ACCOUNT_RULE], now=T0).call_count, 1)
        self.assertEqual(self.run_engine(driver, [ACCOUNT_RULE], now=T0 + 10 * 60).call_count, 0)

    def test_alert_fires_again_after_cooldown(self):
        driver = FakeDriver(accounts=[{"key": "A1", "score": 0.9, "community": 7}])
        self.run_engine(driver, [ACCOUNT_RULE], now=T0)
        self.assertEqual(self.run_engine(driver, [ACCOUNT_RULE], now=T0 + 31 * 60).call_count, 1)

    def test_group_rule_alerts_with_member_count(self):
        driver = FakeDriver(groups=[{"key": 3, "score": 0.9, "members": 51}])
        dispatch = self.run_engine(driver, [GROUP_RULE], now=T0)
        self.assertEqual(dispatch.call_count, 1)
        self.assertIn("51 accounts", dispatch.call_args[0][1])

    def test_each_account_has_its_own_cooldown(self):
        driver = FakeDriver(accounts=[{"key": "A1", "score": 0.9, "community": 7}])
        self.run_engine(driver, [ACCOUNT_RULE], now=T0)
        driver.accounts.append({"key": "A2", "score": 0.95, "community": 7})
        dispatch = self.run_engine(driver, [ACCOUNT_RULE], now=T0 + 60)
        self.assertEqual(dispatch.call_count, 1)
        self.assertIn("A2", dispatch.call_args[0][1])


if __name__ == "__main__":
    unittest.main()
