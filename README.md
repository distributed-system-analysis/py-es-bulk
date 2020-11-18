# py-es-bulk
A simple wrapper around the Python `elasticsearch` client `put_template()`, `streaming_bulk()`, and `parallel_bulk()` helper APIs with robust error handling.

This library is designed to work across various versions of the
elasticsearch Python module and of the Elasticsearch server, by
dynamically identifying the module used to create the `Elasticsearch` object.

These names are available for import:

* `put_template`

    Push a document template to the server using a specified
    Elasticsearch object. This module will determine whether
    a template document of the same name and version already
    exists, and PUT the new template if not.

    Args:
    - `es`: An instance of the `Elasticsearch` class.
    - `name`: The name of the template.
    - `mapping_name`: The name of the mapping used in the template.
    - `body`: The payload body of the template.

    Returns: A tuple (start_time, end_time, retry_count, error_keys)

* `streaming_bulk`

    Push multiple source documents to Elasticsearch indices,
    using proper error handling and retry logic.

    Args:
    - 'es': An instance of the `Elasticsearch` class.
    - `actions`: An iterable of Elasticsearch action records (passed directly to Elasticsearch).
    - `errorsfp`: A file pointer where HTTP 400 errors are logged.
    - `logger`: A `Logger` object where messages can be logged.

    Returns: A tuple (start_time, end_time, successfully_indexed, duplicate, failed, retry_count).


* `parallel_bulk`

    Push multiple source documents to Elasticsearch indices
    in parallel across multiple threads, using proper error
    handling and retry logic.

    Args:
    - `es`: An instance of the `Elasticsearch` class.
    - `actions`: An iterable of Elasticsearch action records
    (passed directly to Elasticsearch)
    - `errorsfp`: A file pointer where HTTP 400 errors are logged.
    - `logger`: A `Logger` object where messages can be logged.
    - `chunk_size=10000000`: Number of docs sent in one chunk to Elasticsearch.
    - `max_chunk_bytes=104857600`: The maximum size of a request.
    - `thread_count=8`: The size of the thread pool to use.
    - `queue_size=4`: The size of the task queue between the controller and processing threads.

    Returns: A tuple (start_time, end_time, successfully_indexed, duplicate, failed, retry_count)

* `TemplateException`

    This exception is raised by put_template when a
    template document does not contain the required version
    metadata (`{"_meta": {"version": <integer>}}`); or, when
    multiple template documents are included in a single call
    to put_template, if the versions of those documents are
    not all identical.

__Unit testing support__

The `pyesbulk` package attempts to dynamically determine the
Python module used to produce the `Elasticsearch` object that's passed in to `pyesbulk` methods. This is necessary in order to properly resolve exception classes for the error handling and retry logic.

However, unit tests often work with mocked objects which
won't have "real" Python package structure, and the dynamic
module recognition algorithm may fail. When this happens,
`pyesbulk` will attempt to import `elasticsearch`. If that's not correct (e.g., if you're using `elasticsearch1`
or `elasticsearch5`), you can override the automatic search
by including a `force_elastic_search_module` property on
your mocked `Elasticsearch` object.

For example,

```
class MockElasticsearch:
    def __init__(self):
        self.force_elastic_search_module = "elasticsearch5"
```

or

```
    es = MockElasticSearch()
    es.force_elastic_search_module = "elasticsearch1"
```


See also https://pypi.org/project/pyesbulk/.
