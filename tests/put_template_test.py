import time
import pytest
from elasticsearch import exceptions as es_excs

from pyesbulk import put_template, TemplateException


class MockException(Exception):
    pass


class MockElasticsearch():
    def __init__(self, mock):
        self.indices = mock


class MockTemplateApis():
    def __init__(self, behavior=None, mapping_name=None, version=42):
        self.name = None
        self.body = None
        self.behavior = behavior
        self.mapping_name = mapping_name
        self.version = 42

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
            elif behavior in ("500", "501", "502", "503", "504"):
                raise es_excs.TransportError(
                    int(behavior), "fake 50x", Exception()
                )
        return None

    def get_template(self, *args, **kwargs):
        assert 'name' in kwargs
        name = kwargs['name']
        tmpl = dict()
        if self.mapping_name is not None:
            tmpl[name] = dict(mappings=dict())
            tmpl[name]['mappings'][self.mapping_name] = dict(
                _meta=dict(version=self.version)
            )
        else:
            # Empty dict indicates template not found
            pass
        return tmpl


class MyTime():
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
        return

    monkeypatch.setattr(time, 'sleep', mysleep)


def test_put_template():
    # Assert that a one-n-done call works as expected
    mpt = MockTemplateApis()
    es = MockElasticsearch(mpt)
    mappings = {'prefix-mapname0': {'_meta': {'version': 0}}}
    body = dict(one=1, two=2, mappings=mappings)
    res = put_template(
        es, name="mytemplate", mapping_name="prefix-mapname0", body=body
    )
    beg, end, retry_count, note = res
    assert beg == 0
    assert end == 1
    assert retry_count == 0
    assert note == "original-no-version"
    assert mpt.name == "mytemplate"
    assert mpt.body == body


def test_put_template_noname():
    # Assert that there's nothing requiring a doc type
    mpt = MockTemplateApis()
    es = MockElasticsearch(mpt)
    mappings = {'_meta': {'version': 0}}
    body = dict(one=1, two=2, mappings=mappings)
    res = put_template(
        es, name="mytemplate", mapping_name="prefix-mapname0", body=body
    )
    beg, end, retry_count, note = res
    assert beg == 0
    assert end == 1
    assert retry_count == 0
    assert note == "original-no-version"
    assert mpt.name == "mytemplate"
    assert mpt.body == body


def test_put_template_no_meta():
    mpt = MockTemplateApis()
    es = MockElasticsearch(mpt)
    mappings = {'_meta': {'versio': 0}}
    body = dict(one=1, two=2, mappings=mappings)
    with pytest.raises(TemplateException):
        put_template(
            es, name="mytemplate", mapping_name="prefix-mapname0", body=body
        )


def test_put_template_named_no_meta():
    mpt = MockTemplateApis()
    es = MockElasticsearch(mpt)
    mappings = {'pbench-run': {'meta': {"version": 0}}}
    body = dict(one=1, two=2, mappings=mappings)
    with pytest.raises(TemplateException):
        put_template(
            es, name="mytemplate", mapping_name="prefix-mapname0", body=body
        )


def test_put_template_not_retried():
    # Assert that a 500 error is raised as a TransportError exception
    mpt = MockTemplateApis(behavior=["501"])
    es = MockElasticsearch(mpt)
    mappings = {'prefix-mapname0': {'_meta': {'version': 0}}}
    body = dict(one=1, two=2, mappings=mappings)
    with pytest.raises(es_excs.TransportError):
        put_template(
            es, name="mytemplate", mapping_name="prefix-mapname0", body=body
        )
    assert mpt.name == "mytemplate"
    assert mpt.body == body


def test_put_template_retries():
    # Assert that ConnectionErrors and TransportErrors are properly retried
    # until successful.
    mpt = MockTemplateApis(behavior=["ce", "ce", "ce", "500", "503", "504"])
    es = MockElasticsearch(mpt)
    mappings = {'prefix-mapname0': {'_meta': {'version': 0}}}
    body = dict(one=1, two=2, mappings=mappings)
    res = put_template(
        es, name="mytemplate", mapping_name="prefix-mapname0", body=body
    )
    beg, end, retry_count, note = res
    assert beg == 0
    assert end == 1
    assert retry_count == 6
    assert note == "original-no-version"
    assert mpt.name == "mytemplate"
    assert mpt.body == body


def test_put_template_exc():
    # Assert that if es.indices.put_template() raises an unknown exception
    # it is passed along to the caller.
    mpt = MockTemplateApis(behavior=["exception"])
    es = MockElasticsearch(mpt)
    mappings = {'prefix-mapname0': {'_meta': {'version': 0}}}
    body = dict(one=1, two=2, mappings=mappings)
    with pytest.raises(MockException):
        put_template(
            es, name="mytemplate", mapping_name="prefix-mapname0", body=body
        )
    assert mpt.name == "mytemplate"
    assert mpt.body == body
