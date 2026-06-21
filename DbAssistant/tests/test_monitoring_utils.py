import monitoring.monitoring_utils as monitoring_utils


def _clear_key(key):
    # best-effort clear of any existing state to avoid cross-test interference
    try:
        monitoring_utils._store.pop(key, None)
    except Exception:
        pass


def test_sustained_breach_greater_than():
    key = "mu_gt_testkey"
    _clear_key(key)

    # window=2 => need two consecutive breaches > 10
    assert monitoring_utils.sustained_breach(key, 5, ">", 10, window=2) is False
    assert monitoring_utils.sustained_breach(key, 12, ">", 10, window=2) is False
    assert monitoring_utils.sustained_breach(key, 15, ">", 10, window=2) is True


def test_sustained_breach_less_than():
    key = "mu_lt_testkey"
    _clear_key(key)

    # window=3 => need three consecutive breaches < 100
    assert monitoring_utils.sustained_breach(key, 120, "<", 100, window=3) is False
    assert monitoring_utils.sustained_breach(key, 90, "<", 100, window=3) is False
    assert monitoring_utils.sustained_breach(key, 80, "<", 100, window=3) is False
    assert monitoring_utils.sustained_breach(key, 70, "<", 100, window=3) is True
