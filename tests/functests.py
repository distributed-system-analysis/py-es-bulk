import sys
import json
import hashlib

from elasticsearch import Elasticsearch
from pyesbulk import streaming_bulk

if __name__ == '__main__':
    _op_type = "create"

    def _make_source_id(source):
        return hashlib.md5(
            json.dumps(source, sort_keys=True).encode('utf-8')
        ).hexdigest()

    def fake_gen():
        for data in range(0, 10):
            source = {'data': data}
            action = {
                _op_type: {
                    "_index":   'test-pyesbulk-streaming_bulk',
                    "_type":    "testrec",
                    "_id":      _make_source_id(source)
                    }
                }
            yield action, source

    es_client = Elasticsearch(
        [{"host": sys.argv[1], "port": sys.argv[2]}], max_retries=0
    )
    with open("errors.json", "w") as errorfp:
        res = streaming_bulk(es_client, fake_gen(), errorfp)
    print(repr(res))
