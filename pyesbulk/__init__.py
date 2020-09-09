"""
Opinionated methods for interfacing with Elasticsearch. We provide two
such opinions for creating templates (put_template()) and bulk indexing
(streaming_bulk).
"""

import json
import logging
import math
import time

from collections import Counter, deque
from datetime import datetime, tzinfo, timedelta
from random import SystemRandom

try:
    from elasticsearch1 import (
        VERSION as es_VERSION, helpers, exceptions as es_excs
    )
    _es_logger = "elasticsearch1"
except ImportError:
    from elasticsearch import (
        VERSION as es_VERSION, helpers, exceptions as es_excs
    )
    _es_logger = "elasticsearch"
assert es_VERSION[0] == 1, (
    "INTERNAL ERROR: only Elasticsearch V1 client is currently supported."
)

# Version of py-es-bulk
__VERSION__ = "1.0.0"

# Use the random number generator provided by the host OS to calculate our
# random backoff.
_r = SystemRandom()
# The maximum amount of time (in seconds) we'll wait before retrying an
# operation.
_MAX_SLEEP_TIME = 120
# Always use "create" operations, as we also ensure each JSON document being
# indexed has an "_id" field, so we can tell when we are indexing duplicate
# data.
_op_type = "create"
# 100,000 minute timeout talking to Elasticsearch; basically we just don't
# want to timeout waiting for Elasticsearch and then have to retry, as that
# can add undue burden to the Elasticsearch cluster.
_request_timeout = 100000*60.0
# Maximum length of messages logged by streaming_bulk()
_MAX_ERRMSG_LENGTH = 16384


class simple_utc(tzinfo):
    def tzname(self, *args, **kwargs):
        return "UTC"

    def utcoffset(self, dt):
        return timedelta(0)

    def dst(self, dt):
        return timedelta(0)


def _tstos(ts=None):
    if ts is None:
        ts = time.time()
    dt = datetime.utcfromtimestamp(ts).replace(tzinfo=simple_utc())
    return dt.strftime("%Y-%m-%dT%H:%M:%S-%Z")


def _calc_backoff_sleep(backoff):
    global _r
    b = math.pow(2, backoff)
    return _r.uniform(0, min(b, _MAX_SLEEP_TIME))


def _sleep_w_backoff(backoff):
    time.sleep(_calc_backoff_sleep(backoff))


def quiet_loggers():
    """
    A convenience function to quiet the urllib3 and elasticsearch1 loggers.
    """
    logging.getLogger("urllib3").setLevel(logging.FATAL)
    logging.getLogger(_es_logger).setLevel(logging.FATAL)


# The 5xx codes on which template PUT operations are retried.
_RETRY_5xxS = [500, 503, 504]


def put_template(es, name=None, mapping_name=None, body=None):
    """
    put_template(es, name, mapping_name, body)

    Updates a given template when the version of the template as
    stored in the mapping is different from the existing one.

    Arguments:

        es - An Elasticsearch client object already constructed
        name - The name of the template to use
        mapping_name - The name of the mapping used in the template
        body - The payload body of the template

    Returns:

        A tuple with the start and end times of the PUT operation, along
        with the number of times the operation was retried, and string
        of keywords indicating note-worthy behavior (currently only
        "original-no-version", which is returned when the mapping being
        updated has no version number).

        Failure modes are raised as exceptions.
    """
    assert name is not None and mapping_name is not None and body is not None
    retry = True
    retry_count = 0
    original_no_version = False
    backoff = 1
    beg, end = time.time(), None
    try:
        body_ver = int(body["mappings"][mapping_name]["_meta"]["version"])
    except KeyError:
        raise Exception(
            f"Bad template, {name}: mapping name, '{mapping_name}',"
            " missing from template"
        )
    while retry:
        original_no_version = False  # Initialized twice for proper scope
        try:
            tmpl = es.indices.get_template(name=name)
        except es_excs.ConnectionError:
            # We retry all connection errors
            _sleep_w_backoff(backoff)
            backoff += 1
            retry_count += 1
            continue
        except es_excs.TransportError as exc:
            if exc.status_code == 404:
                # We expected a "not found", we'll PUT below.
                pass
            elif exc.status_code in _RETRY_5xxS:
                # Only retry on certain 5xx errors
                _sleep_w_backoff(backoff)
                backoff += 1
                retry_count += 1
                continue
            else:
                # All other error codes are some kind of error we don't attempt
                # to retry.
                raise
        else:
            try:
                tmpl_ver = int(
                    tmpl[name]["mappings"][mapping_name]["_meta"]["version"]
                )
            except KeyError:
                original_no_version = True
            else:
                if tmpl_ver == body_ver:
                    break
        try:
            es.indices.put_template(name=name, body=body)
        except es_excs.ConnectionError:
            # We retry all connection errors
            _sleep_w_backoff(backoff)
            backoff += 1
            retry_count += 1
        except es_excs.TransportError as exc:
            if exc.status_code < 500:
                # Somehow the PUT payload was in error, don't retry.
                raise
            elif exc.status_code in _RETRY_5xxS:
                # Only retry on certain 500 errors
                _sleep_w_backoff(backoff)
                backoff += 1
                retry_count += 1
            else:
                # All other error codes are some kind of error we don't attempt
                # to retry.
                raise
        else:
            retry = False
    end = time.time()
    note = "original-no-version" if original_no_version else ""
    return beg, end, retry_count, note


def streaming_bulk(es, actions, errorsfp, logger):
    """
    streaming_bulk(es, actions, errorsfp, logger)

    Arguments:

        es - An Elasticsearch client object already constructed
        actions - An iterable for the documents to be indexed
        errorsfp - A file pointer for where to write 400 errors
        logger - A python logging object to use to report behaviors;
                 (the logger is expected to handle {} formatting)

    Returns:

        A tuple with the start and end times, the # of successfully indexed,
        duplicate, and failed documents, along with number of times a bulk
        request was retried.
    """

    # These need to be defined before the closure below. These work because
    # a closure remembers the binding of a name to an object. If integer
    # objects were used, the name would be bound to that integer value only
    # so for the retries, incrementing the integer would change the outer
    # scope's view of the name.  By using a Counter object, the name to
    # object binding is maintained, but the object contents are changed.
    actions_deque = deque()
    actions_retry_deque = deque()
    retries_tracker = Counter()

    def actions_tracking_closure(cl_actions):
        for cl_action in cl_actions:
            for field in ("_id", "_index", "_type"):
                assert (
                    field in cl_action
                ), f"Action missing '{field}' field: {cl_action!r}"
            assert _op_type == cl_action["_op_type"], (
                "Unexpected _op_type"
                f" value \"{cl_action['_op_type']}\" in action {cl_action!r}"
            )

            # Append to the right side ...
            actions_deque.append((0, cl_action))
            yield cl_action
            # If after yielding an action some actions appear on the retry
            # deque, start yielding those actions until we drain the retry
            # queue.
            backoff = 1
            while len(actions_retry_deque) > 0:
                _sleep_w_backoff(backoff)
                retries_tracker["retries"] += 1
                retry_actions = []
                # First drain the retry deque entirely so that we know when we
                # have cycled through the entire list to be retried.
                while len(actions_retry_deque) > 0:
                    retry_actions.append(actions_retry_deque.popleft())
                for retry_count, retry_action in retry_actions:
                    # Append to the right side ...
                    actions_deque.append((retry_count, retry_action))
                    yield retry_action
                # If after yielding all the actions to be retried, some show
                # up on the retry deque again, we extend our sleep backoff to
                # avoid pounding on the ES instance.
                backoff += 1

    beg, end = time.time(), None
    successes = 0
    duplicates = 0
    failures = 0

    # Create the generator that closes over the external generator, "actions"
    generator = actions_tracking_closure(actions)

    streaming_bulk_generator = helpers.streaming_bulk(
        es,
        generator,
        raise_on_error=False,
        raise_on_exception=False,
        request_timeout=_request_timeout,
    )

    for ok, resp_payload in streaming_bulk_generator:
        retry_count, action = actions_deque.popleft()
        try:
            resp = resp_payload[_op_type]
        except KeyError as e:
            assert not ok, f"ok = {ok!r}, e = {e!r}"
            assert (
                e.args[0] == _op_type
            ), f"e.args = {e.args!r}, _op_type = {_op_type!r}"
            # For whatever reason, some errors are always returned using
            # the "index" operation type instead of _op_type (e.g. "create"
            # op type still comes back as an "index" response).
            try:
                resp = resp_payload["index"]
            except KeyError:
                # resp is not of expected form; set it to the complete
                # payload, so that it can be reported properly below.
                resp = resp_payload
        try:
            status = resp["status"]
        except KeyError as e:
            assert not ok, f"ok = {ok!r}, e = {e!r}"
            logger.error("{!r}", e)
            status = 999
        else:
            assert action["_id"] == resp["_id"], (
                "Response encountered out of order from actions, "
                f"action = {action!r}, response = {resp!r}"
            )
        if ok:
            successes += 1
        else:
            if status == 409:
                if retry_count == 0:
                    # Only count duplicates if the retry count is 0 ...
                    duplicates += 1
                else:
                    # ... otherwise consider it successful.
                    successes += 1
            elif status == 400:
                try:
                    exc_payload = resp["exception"]
                except KeyError:
                    pass
                else:
                    # We have an exception object in the response object
                    # which is not always JSON serializable, so we use
                    # `repr` to turn that exception into a serializable
                    # string while maintaining as much information about
                    # the exception as possible.
                    resp["exception"] = repr(exc_payload)
                jsonstr = json.dumps(
                    {
                        "action": action,
                        "ok": ok,
                        "resp": resp,
                        "retry_count": retry_count,
                        "timestamp": _tstos(),
                    },
                    indent=4,
                    sort_keys=True,
                )
                print(jsonstr, file=errorsfp)
                errorsfp.flush()
                failures += 1
            else:
                try:
                    exc_payload = resp["exception"]
                except KeyError:
                    pass
                else:
                    resp["exception"] = repr(exc_payload)
                try:
                    error = resp["error"]
                except KeyError:
                    error = ""
                if status == 403 and error.startswith("IndexClosedException"):
                    # Don't retry closed index exceptions
                    jsonstr = json.dumps(
                        {
                            "action": action,
                            "ok": ok,
                            "resp": resp,
                            "retry_count": retry_count,
                            "timestamp": _tstos(),
                        },
                        indent=4,
                        sort_keys=True,
                    )
                    print(jsonstr, file=errorsfp)
                    errorsfp.flush()
                    failures += 1
                else:
                    # Retry all other errors.
                    # Limit the length of the warning message.
                    logger.warning(
                        "retrying action: {}",
                        json.dumps(resp)[:_MAX_ERRMSG_LENGTH]
                    )
                    actions_retry_deque.append((retry_count + 1, action))

    end = time.time()

    if len(actions_deque) > 0:
        logger.error(
            "We still have {:d} actions in the deque", len(actions_deque)
        )
    if len(actions_retry_deque) > 0:
        logger.error(
            "We still have {:d} retry actions in the deque",
            len(actions_retry_deque)
        )

    return (
        beg, end, successes, duplicates, failures, retries_tracker['retries']
    )
