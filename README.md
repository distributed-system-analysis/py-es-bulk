# py-es-bulk
A simple wrapper around elasticsearch-py client streaming_bulk() API with
robust error handling.

This library is designed to work across various versions of the
elasticsearch Python module and of the Elasticsearch server, as well as
Elasticsearch V1 with the elasticsearch1 Python module.

These names are available for import:

* put_template

    Push a document template to the server using a specified
    Elasticsearch object. This module will determine whether
    a template document of the same name and version already
    exists, and PUT the new template if not.

* streaming_bulk

    Push multiple source documents to Elasticsearch indices,
    using proper error handling and retry logic.

* parallel_bulk

    Push multiple source documents to Elasticsearch indices
    in parallel across multiple threads, using proper error
    handling and retry logic.

* TemplateException

    This exception is raised by put_template when a
    template document does not contain the required version
    metadata ({"_meta": {"version": <integer>}}); or, when
    multiple template documents are included in a single call
    to put_template, if the versions of those documents are
    not all identical.

See also https://pypi.org/project/pyesbulk/.
