import io
import json
import time
import logging
from collections import Counter
from elasticsearch import helpers

from pyesbulk import parallel_bulk
from tests.put_template_test import MyTime


class MockElasticsearch():
    def __init__(self):
        pass


class MockStreamingBulk():
    def __init__(self, max_actions):
        self.max_actions = max_actions
        self.actions_l = []
        self.duplicates_tracker = Counter()
        self.index_tracker = Counter()
        self.dupes_by_index_tracker = Counter()

    def parallel_bulk(self, es, actions, **kwargs):
        assert 'thread_count' in kwargs
        assert 'queue_size' in kwargs
        assert 'chunk_size' in kwargs
        assert 'max_chunk_bytes' in kwargs
        for action in actions:
            self.duplicates_tracker[action['_id']] += 1
            dcnt = self.duplicates_tracker[action['_id']]
            if dcnt == 2:
                self.dupes_by_index_tracker[action['_index']] += 1
            self.index_tracker[action['_index']] += 1
            if self.index_tracker[action['_index']] <= self.max_actions:
                self.actions_l.append(action)
            resp = {}
            resp[action['_op_type']] = {'_id': action['_id']}
            if dcnt > 2:
                # Report each duplicate
                resp[action['_op_type']]['status'] = 409
                ok = False
            else:
                # For now, all other docs are considered successful
                resp[action['_op_type']]['status'] = 200
                ok = True
            yield ok, resp

    def report(self):
        for idx in sorted(self.index_tracker.keys()):
            print("Index: ", idx, self.index_tracker[idx])
        total_dupes = 0
        total_multi_dupes = 0
        for docid in self.duplicates_tracker:
            total_dupes += 0 if (
                    self.duplicates_tracker[docid] <= 1
                ) else self.duplicates_tracker[docid]
            if self.duplicates_tracker[docid] >= 2:
                total_multi_dupes += 1
        if total_dupes > 0:
            print(
                f"Duplicates: {total_dupes},"
                f" Multiple dupes: {total_multi_dupes}"
            )
        for idx in sorted(self.dupes_by_index_tracker.keys()):
            print("Index dupes: ", idx, self.dupes_by_index_tracker[idx])
        print("len(actions) = {}".format(len(self.actions_l)))
        print(json.dumps(self.actions_l, indent=4, sort_keys=True))


def test_parallel_bulk(monkeypatch):
    es = MockElasticsearch()
    mock = MockStreamingBulk(15)
    with monkeypatch.context() as m:
        clock = MyTime()

        def mytime():
            return clock.tick()

        m.setattr(time, 'time', mytime)

        def mysleep(*args, **kwargs):
            return

        m.setattr(time, 'sleep', mysleep)
        m.setattr(helpers, 'parallel_bulk', mock.parallel_bulk)
        with io.StringIO() as errorfp:
            parallel_bulk(es, [], errorfp, logging.getLogger())
