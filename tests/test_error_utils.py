# -*- coding: utf-8 -*-
from orchestrator import error_utils


def test_same_root_cause_different_line_numbers_hash_equal():
    a = "Traceback: foo.py:42: AssertionError: expected 3 got 4"
    b = "Traceback: foo.py:57: AssertionError: expected 3 got 4"
    assert error_utils.error_hash(a) == error_utils.error_hash(b)


def test_same_root_cause_different_timestamps_hash_equal():
    a = "2026-01-04T10:22:31Z test_billing failed: total mismatch"
    b = "2026-01-04T11:58:02Z test_billing failed: total mismatch"
    assert error_utils.error_hash(a) == error_utils.error_hash(b)


def test_same_root_cause_different_tmp_paths_hash_equal():
    a = "cannot write to /tmp/build-9f8e3a21/out.log"
    b = "cannot write to /tmp/build-2b71c904/out.log"
    assert error_utils.error_hash(a) == error_utils.error_hash(b)


def test_genuinely_different_errors_hash_differently():
    a = "AssertionError: expected 3 got 4"
    b = "TypeError: unsupported operand type(s) for +: 'int' and 'str'"
    assert error_utils.error_hash(a) != error_utils.error_hash(b)


def test_is_similar_error_fallback():
    a = "KeyError: 'user_id_123'"
    b = "KeyError: 'user_id_456'"
    assert error_utils.is_similar_error(a, b)
