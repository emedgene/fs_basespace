fs\_basespace
=============

*Readonly `pyfilesystem2 <https://github.com/PyFilesystem/pyfilesystem2>` interface to `Illumina Basespace <https://developer.basespace.illumina.com/docs/content/documentation/sdk-samples/python-sdk-overview>`*

Installing
----------

::
    pip install fs-basespace

Opening FS Basespace
--------------------

With class constructor

.. code-block:: python
    from fs_basespace import BASESPACEFS
    basespacefs = BASESPACEFS("/projects/{project-id}/appresults/{result-id}/files/{file-id}",
                              client_id = "{client-key}",
                              client_secret = "{client-secret}",
                              access_token = "{access_token}",
                              server_name = "{server_name}")

Default server name for Illumina is `https://api.basespace.illumina.com/`_


With connection string

.. code-block:: python
    import fs
    basespacefs = fs.open_fs("basespace://{clientKey}:{clientSecret}:{appToken}@{server}")

Advanced connection strings
---------------------------

Access to server root directory:

::

    basespace://{clientKey}:{clientSecret}:{appToken}@{server}

Accessing projects:

::
    basespace://{clientKey}:{clientSecret}:{appToken}@{server}!/projects/{projectId}

Accessing project sample files:

::
    basespace://{clientKey}:{clientSecret}:{appToken}@{server}!/projects/{projectId}/samples/{sampleId}/files/{fileId}

Accessing project appResult files (bam, vcf, ...):

::
    basespace://{clientKey}:{clientSecret}:{appToken}@{server}!/projects/{projectId}/appresults/{resultId}/files/{fileId}


Downloading files
-----------------

.. code-block:: python

    with open("local_file", "wb") as local_file:
        basespacefs.download("path/to/remote/file/id", local_file)


Uploading files
-----------------

    Only readonly access to Basespace is implemented in this package. No upload possible yet.
