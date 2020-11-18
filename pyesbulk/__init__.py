"""
Opinionated methods for interfacing with Elasticsearch. We provide two
such opinions for creating templates (put_template()) and bulk indexing
(streaming_bulk).
"""

import json
import logging
import math
import time
import inspect
import importlib
from pathlib import Path

from collections import Counter, deque
from datetime import datetime, tzinfo, timedelta
from random import SystemRandom


class TemplateException(Exception):
    """
    TemplateException This is raised to report problems with the template
    payloads provided to put_template.

    The client called put_template with a template body document that does not
    contain a {"_meta": {"version": <int>}} clause, or a body containing more
    than one template document which have differing versions.

    The text representation of the exception will contain details.
    """
    pass


def _import_elasticsearch(es, logger):
    """
    _import_elasticsearch Import the necessary Elasticsearch attributes from
    the Elasticsearch module used to create an Elasticsearch instance, and
    binds the 'exceptions' sub-module globally as "es_excs" and the 'helpers'
    sub-module globally as 'helpers'.

    NOTE: simply importing 'elasticsearch' won't work if the caller is using
    'elasticsearch1' (or vice versa), because the 'es_excs' exception classes
    won't match our 'except' clauses, and template registration won't work!

    Args:
        es (Elasticsearch): An Elasticsearch object
        logger: A logger object
    """
    module = None
    try:
        module = es.force_elastic_search_module
    except AttributeError:
        # No override was specified, so we'll look for a real module path
        pass

    if not module:
        # Get the file path of the Elasticsearch class
        es_import = Path(inspect.getfile(type(es)))

        # Walk up the file path to the directory that contains
        # "elasticsearch" ... this should be either "elasticsearch1" for a V1
        # or "elasticsearch" for a later version.
        #
        # In a unit test environment, we won't recognize the mock path, and
        # for that and as a fallback, if we get to the root without finding
        # a match, just import 'elasticsearch'.
        path = es_import.parent
        while "elasticsearch" not in path.name and path.name != "":
            path = path.parent
        module = path.name
        if module == "":
            logger.info(
                """Unable to determine Elasticsearch module algorithmically;
                you can override the default 'elasticsearch' by setting a
                'force_elastic_search_module' property on your Elasticsearch()
                object; for example, if 'es = Elasticsearch()' then
                'es.force_elastic_search_module=elasticsearch1'
                """
            )
            module = "elasticsearch"

    # Dymamically import the module so that we can identify, import, and
    # bind the sub-modules we need.
    es_module = importlib.import_module(module)

    # I don't know why 'exceptions' is visible as an attribute of the module
    # but 'helpers' isn't. Therefore, the import mechanism is different. We are
    # manually binding global names matching the original static imports.
    # g = globals()
    global es_excs, helpers
    es_excs = importlib.import_module(
        getattr(es_module, "exceptions").__name__
    )
    helpers = importlib.import_module(es_module.__name__ + ".helpers")


# Version of py-es-bulk
__VERSION__ = "2.1.1"

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
_request_timeout = 100000 * 60.0
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
    logging.getLogger("elasticsearch").setLevel(logging.FATAL)


# The 5xx codes on which template PUT operations are retried.
_RETRY_5xxS = [500, 503, 504]


def _get_meta_version(name, body):
    """
    _get_meta_version Try to find an "_meta":{"version": "value"} in the
    specified template body.

    For V7 and beyond, a template should not have a name, which means the
    mapping body includes an "_meta" key directly.

    For V6 and V7 there may be an "_doc" key in the top level dict, which
    will have "_meta" as a second level key.

    Earlier versions will normally have a "document type" as the first
    level key, with "_meta" underneath.

    We're being accommodating here: if the first level dict includes an
    "_meta" key, we'll use it. If not, and the first level dict has a single
    key, we'll look for "_meta" under that.

    Args:
        body (dict): An Elasticsearch index document template "mappings"
        document.
    """
    if not isinstance(body, dict):
        raise TypeError
    underbody = body
    version = None
    try:
        if "_meta" not in body:
            # Elasticsearch V1 allows multiple named templates in an index;
            # we require that the versions be the same since we can return
            # only one.
            underbody = body[next(iter(body))]
            for key in body:
                v = int(body[key]["_meta"]["version"])
                if not version:
                    version = v
                else:
                    if v != version:
                        raise TemplateException(
                            f"Bad template, {name}: multiple templates with "
                            "differing versions at {key}"
                        )
        else:
            version = int(underbody["_meta"]["version"])
    except KeyError:
        raise TemplateException(
            f"Bad template, {name}: '_meta version' missing from template"
        )
    return version


def put_template(
        es, name=None, mapping_name=None, body=None
):
    """
    put_template(es, name, mapping_name, body)

    Updates a given template when the version of the template as
    stored in the mapping is different from the existing one.

    put_template requires that each template document contain a "_meta"
    clause with an integer "version" field, e.g.,

        {"_meta": {"version": 4}, "properties": {...}}

    If the `put_template` body contains multiple template documents, they
    must each have a version, and those versions must all be the same.

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

    Raises:

        TemplateException signals that a template is not compatible with the
        format pyesbulk expects. (Specifically with the version metadata
        requirements described above.)
    """
    assert name is not None and mapping_name is not None and body is not None
    logger = logging.getLogger()
    _import_elasticsearch(es, logger)
    retry = True
    retry_count = 0
    original_no_version = False
    backoff = 1
    beg, end = time.time(), None
    mapping = body["mappings"]
    body_ver = _get_meta_version(name, mapping)
    while retry:
        original_no_version = False  # Initialized twice for proper scope
        try:
            tmpl = es.indices.get_template(name=name)
        except es_excs.NotFoundError as exc:
            if exc.status_code == 404:
                # We expected a "not found", we'll PUT below.
                pass
            else:
                # Not sure what to do here? Can it be anything except 404?
                raise
        except es_excs.ConnectionError:
            # We retry all connection errors
            _sleep_w_backoff(backoff)
            backoff += 1
            retry_count += 1
            continue
        except es_excs.TransportError as exc:
            if exc.status_code == 404:
                # We expected a "not found", we'll PUT below. NOTE: this may
                # trigger on Elasticsearch 1, but on Elasticsearch 7 a 404
                # is instead packaged as a NotFoundError exception!
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
        except Exception as e:
            logger.error(
                "Unexpected exception checking %s: %r", name, e
            )
            raise
        else:
            try:
                tmpl_ver = _get_meta_version(name, tmpl[name]["mappings"])
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
        except Exception as e:
            logger.error(
                "Unexpected exception checking %s: %r", name, e
            )
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
    _import_elasticsearch(es, logger)
    return _internal_bulk(
        es, actions, errorsfp, helpers.streaming_bulk, logger
    )


def parallel_bulk(
        es, actions, errorsfp, logger, chunk_size=10000000,
        max_chunk_bytes=104857600, thread_count=8, queue_size=4
        ):
    """
    parallel_bulk(es, actions, errorsfp, logger, chunk_size=10000000,
        max_chunk_bytes=104857600, thread_count=8, queue_size=4)

    Arguments:

        es - An Elasticsearch client object already constructed
        actions - An iterable for the documents to be indexed
        errorsfp - A file pointer for where to write 400 errors
        logger - A python logging object to use to report behaviors;
                 (the logger is expected to handle {} formatting)
        chunk_size -
        max_chunk_bytes -
        thread_count -
        queue_size -

    Returns:

        A tuple with the start and end times, the # of successfully indexed,
        duplicate, and failed documents, along with number of times a bulk
        request was retried.
    """
    _import_elasticsearch(es, logger)
    return _internal_bulk(
        es, actions, errorsfp, helpers.parallel_bulk, logger,
        chunk_size=chunk_size, max_chunk_bytes=max_chunk_bytes,
        thread_count=thread_count, queue_size=queue_size
    )


def _internal_bulk(es, actions, errorsfp, bulk_method, logger, **kwargs):
    """
    _internal_bulk(es, actions, errorsfp, bulk_method, logger, **kwargs)

    Arguments:

        es - An Elasticsearch client object already constructed
        actions - An iterable for the documents to be indexed
        errorsfp - A file pointer for where to write 400 errors
        bulk_method - The `helpers.*` bulk indexing method, either streaming
                      or parallel
        logger - A python logging object to use to report behaviors;
                 (the logger is expected to handle {} formatting)
        **kwargs - Passed along to the given bulk_method

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
            for field in ("_id", "_index"):
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

    streaming_bulk_generator = bulk_method(
        es,
        generator,
        raise_on_error=False,
        raise_on_exception=False,
        request_timeout=_request_timeout,
        **kwargs
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
            "We still have {:d} actions in the deque",
            len(actions_deque)
        )
    if len(actions_retry_deque) > 0:
        logger.error(
            "We still have {:d} retry actions in the deque",
            len(actions_retry_deque)
        )

    return (
        beg, end, successes, duplicates, failures, retries_tracker["retries"]
    )
