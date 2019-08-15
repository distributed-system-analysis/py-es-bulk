import time
import pytest
from elasticsearch1 import exceptions as es_excs

from pyesbulk import put_template


class MockException(Exception):
    pass


class MockElasticsearch(object):
    def __init__(self, mock):
        self.indices = mock


class MockPutTemplate(object):
    def __init__(self, behavior=None):
        self.name = None
        self.body = None
        self.behavior = behavior

    def put_template(self, *args, **kwargs):
        assert 'name' in kwargs and 'body' in kwargs
        name = kwargs['name']
        if self.name is None:
            self.name = name
        else:
            assert self.name == name
        body = kwargs['body']
        if self.body is None:
            self.body = body
        else:
            assert self.body == body
        if self.behavior:
            behavior = self.behavior.pop(0)
            if behavior == "exception":
                raise MockException()
            elif behavior == "ce":
                raise es_excs.ConnectionError(None, "fake ce", Exception())
            elif behavior in ( "500", "501", "502", "503", "504" ):
                raise es_excs.TransportError(int(behavior), "fake 50x", Exception())
        return None


class MyTime(object):
    """Monotonically incrementing time based on the # of times the tick()
    method is called."""
    def __init__(self):
        self._tick = 0
    def tick(self):
        _tick = self._tick
        self._tick += 1
        return _tick

# FIXME: mock _calc_sleep_backoff and track backoff numbers

@pytest.fixture(autouse=True)
def patch_time(monkeypatch):
    clock = MyTime()
    def mytime():
        return clock.tick()
    monkeypatch.setattr(time, 'time', mytime)

@pytest.fixture(autouse=True)
def patch_sleep(monkeypatch):
    def mysleep(*args, **kwargs):
        return;
    monkeypatch.setattr(time, 'sleep', mysleep)

def test_put_template():
    # Assert that a one-n-done call works as expected
    mpt = MockPutTemplate()
    es = MockElasticsearch(mpt)
    body = dict(one=1, two=2)
    res = put_template(es, "mytemplate", body)
    beg, end, retry_count = res
    assert beg == 0
    assert end == 1
    assert retry_count == 0
    assert mpt.name == "mytemplate"
    assert mpt.body == body

def test_put_template_not_retried():
    # Assert that a 500 error is raised as a TransportError exception
    mpt = MockPutTemplate(behavior=["501"])
    es = MockElasticsearch(mpt)
    body = dict(one=1, two=2)
    with pytest.raises(es_excs.TransportError) as excinfo:
        res = put_template(es, "mytemplate", body)
    assert mpt.name == "mytemplate"
    assert mpt.body == body

def test_put_template_retries():
    # Assert that ConnectionErrors and TransportErrors are properly retried
    # until successful.
    mpt = MockPutTemplate(behavior=["ce", "ce", "ce", "500", "503", "504"])
    es = MockElasticsearch(mpt)
    body = dict(one=1, two=2)
    res = put_template(es, "mytemplate", body)
    beg, end, retry_count = res
    assert beg == 0
    assert end == 1
    assert retry_count == 6
    assert mpt.name == "mytemplate"
    assert mpt.body == body

def test_put_template_exc():
    # Assert that if es.indices.put_template() raises an unknown exception
    # it is passed along to the caller.
    mpt = MockPutTemplate(behavior=["exception"])
    es = MockElasticsearch(mpt)
    body = dict(one=1, two=2)
    with pytest.raises(MockException) as excinfo:
        res = put_template(es, "mytemplate", body)
    assert mpt.name == "mytemplate"
    assert mpt.body == body
