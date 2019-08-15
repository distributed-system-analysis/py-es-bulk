import time
import pytest

from pyesbulk import _tstos, _calc_backoff_sleep, _r

@pytest.fixture(name="mockgmtime")
def patch_time(monkeypatch):
    st = time.gmtime(873417700)
    def mygmtime(cls):
        return st
    monkeypatch.setattr(time, 'gmtime', mygmtime)

@pytest.mark.usefixtures("mockgmtime")
def test_tstos():
    assert _tstos() == "1997-09-05T00:01:40-UTC"

def test_tstos_w_param():
    assert _tstos(873417600) == "1997-09-05T00:00:00-UTC"

@pytest.fixture(name="mockuniform")
def patch_uniform(monkeypatch):
    def myuniform(zero, m):
        assert zero == 0
        return m
    monkeypatch.setattr(_r, 'uniform', myuniform)

@pytest.mark.usefixtures("mockuniform")
def test_calc_backoff_sleep():
    assert _calc_backoff_sleep(1) == 2.0
    assert _calc_backoff_sleep(4) == 16.0
    assert _calc_backoff_sleep(7) == 120.0
