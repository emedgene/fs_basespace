from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import io
import os
import threading
import logging
from fs import errors
from fs import ResourceType
from fs import tools
from fs.base import FS
from fs.mode import Mode
from fs.info import Info
from fs.path import normpath
from fs.path import relpath
from smart_open.http import SeekableBufferedInputBase

from .api_factory import BasespaceApiFactory
from .basespace_context import FileContext
from .basespace_context import CategoryContext
from .basespace_context import get_last_direct_context
from .basespace_context import get_context_by_key

__all__ = ["BASESPACEFS"]
_BASESPACE_DEFAULT_SERVER = "https://api.basespace.illumina.com/"

logger = logging.getLogger("BaseSpaceFs")
logger.setLevel(logging.DEBUG)


def _make_repr(class_name, *args, **kwargs):
    """
    Generate a repr string.

    Positional arguments should be the positional arguments used to
    construct the class. Keyword arguments should consist of tuples of
    the attribute value and default. If the value is the default, then
    it won't be rendered in the output.

    Here's an example::

        def __repr__(self):
            return make_repr('MyClass', 'foo', name=(self.name, None))

    The output of this would be something line ``MyClass('foo',
    name='Will')``.

    """
    arguments = [repr(arg) for arg in args]
    arguments.extend(
        "{}={!r}".format(name, value)
        for name, (value, default) in sorted(kwargs.items())
        if value != default
    )
    return "{}({})".format(class_name, ", ".join(arguments))


class BASESPACEFS(FS):
    def __init__(
            self,
            dir_path="/",
            client_id=None,
            client_secret=None,
            access_token=None,
            basespace_server=None
    ):
        self._prefix = relpath(normpath(dir_path)).rstrip("/")
        self._tlocal = threading.local()

        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.basespace_server = basespace_server or _BASESPACE_DEFAULT_SERVER

        self._validate_mandatory_fields()

        super(BASESPACEFS, self).__init__()
        logger.debug('BaseSpaceFs is created')

    @property
    def basespace(self) -> BasespaceApiFactory:
        if not hasattr(self._tlocal, "basespace"):
            self._tlocal.basesapce_api_factory = BasespaceApiFactory(self.client_id, self.client_secret, self.basespace_server, self.access_token)
        return self._tlocal.basesapce_api_factory

    def __repr__(self):
        return _make_repr(
            self.__class__.__name__,
            self._prefix,
            client_id=(self.client_id, None),
            client_secret=(self.client_secret, None),
            access_token=(self.access_token, None),
        )

    def __str__(self):
        return f"<basespace '{self._prefix}'>"

    def _validate_mandatory_fields(self):
        if not self.client_id:
            raise ValueError('Client id must be specified')
        if not self.client_secret:
            raise ValueError('Client secret must be specified')
        if not self.access_token:
            raise ValueError('Access token must be specified')
        if not self.basespace_server:
            raise ValueError('Basespace server must be specified')

    @staticmethod
    def _validate_key(key):
        get_last_direct_context(key)

    def _path_to_key(self, path):
        """Converts an fs path to a basespace path."""
        _path = relpath(normpath(path))
        _key = (
            "{}/{}".format(self._prefix, _path).strip("/")
        )
        self._validate_key(_key)
        return _key

    def _get_context_by_key(self, key, page=None):
        return get_context_by_key(self.basespace, key, page)

    def getinfo(self, path, namespaces=None):
        logger.debug(f'getinfo path: {path}')
        if path in ['', '/']:
            raise errors.ResourceNotFound(path)
        namespaces = namespaces or ()
        _path = self.validatepath(path)

        try:
            _key = self._path_to_key(_path)
            current_context = self._get_context_by_key(_key)
            info_dict = self._info_from_object(current_context, namespaces)
        except Exception:
            raise errors.ResourceNotFound(path)

        return Info(info_dict)

    @staticmethod
    def _get_extras(raw_obj):
        if qc_status := getattr(raw_obj, "qc_status", None):
            return {
                "qc_status": qc_status
            }

    def _info_from_object(self, obj, namespaces):
        """ Make an info dict from the basespace context object
            List of functional namespaces: https://github.com/PyFilesystem/pyfilesystem2/blob/master/fs/info.py
        """
        raw_obj = obj.raw_obj
        name = obj.get_id()
        alias = obj.get_name()
        is_dir = not isinstance(obj, FileContext)
        info = {"basic": {"name": name, "is_dir": is_dir, "alias": alias}}

        if isinstance(obj, CategoryContext):
            # it is category context, fake dir to suggest available actions on the entity
            return info

        if "details" in namespaces:
            _type = int(ResourceType.directory if is_dir else ResourceType.file)
            details_info = {
                "type": _type,
                "created": str(obj.get_date_created())
            }
            if extras := self._get_extras(raw_obj):
                details_info["extras"] = extras
            if not is_dir:
                details_info["size"] = obj.get_size()
            info["details"] = details_info

        if "access" in namespaces:
            access_info = dict()
            if is_dir:
                access_info["owner"] = raw_obj.UserOwnedBy
                access_info["permissions"] = raw_obj.getAccessStr(self.basespace).split(" ")[0]
            info["access"] = access_info
        return info

    def scandir(
            self,
            path,  # type: Text     # noqa
            namespaces=None,  # type: Optional[Collection[Text]]    # noqa
            page=None,  # type: Optional[Tuple[int, int]]   # noqa
    ):
        # type: (...) -> Iterator[Info] # noqa
        logger.debug(f'scandir path: {path}')
        namespaces = namespaces or ()
        _path = self.validatepath(path)

        try:
            _key = self._path_to_key(_path)
        except Exception:
            raise errors.ResourceNotFound(path)

        info = (
            Info(self._info_from_object(entity, namespaces=namespaces))
            for entity in self._listdir_entities(_key, page)
        )
        iter_info = iter(info)
        return iter_info

    def _listdir_entities(self, key, page=None):
        destination = self._get_context_by_key(key, page)
        return [entry for entry in destination.list(self.basespace, page)]

    def listdir(self, path):
        logger.debug(f'listdir path: {path}')
        if not self.isdir(path) and not self.isfile(path):
            raise errors.DirectoryExpected(path)

        try:
            _path = self.validatepath(path)
            _key = self._path_to_key(_path)
            entities_list = self._listdir_entities(_key)
        except Exception:
            raise errors.ResourceNotFound(path)

        return sorted([entry.get_id() for entry in entities_list])

    def openbin(self, path, mode="r", buffering=-1, **options):
        _mode = Mode(mode)
        if _mode.create:
            raise errors.ResourceReadOnly

        _mode.validate_bin()

        s3_url = self.geturl(path=path)
        return SeekableBufferedInputBase(s3_url, mode, timeout=15)

    def download(self, path, file, chunk_size=None, **options):
        logger.debug(f'download path: {path}')
        try:
            with self.openbin(path, "rb") as basespace_f:
                tools.copy_file_data(basespace_f, file)
        except Exception as e:
            logger.exception(f'download failed: {path} err: {str(e)}')
            raise

        try:
            self.validate_files_has_same_size(path, file)
        except Exception as e:
            logger.exception(f'download failed: {path} err: {str(e)}')
            raise

    def validate_files_has_same_size(self, path, file):
        current_context = self.get_context_by_path(path)
        file_size_in_path = current_context.raw_obj.Size
        file.seek(0, io.SEEK_END)
        downloaded_file_size = file.tell()
        if file_size_in_path != downloaded_file_size:
            error_msg = f'download failed: {path} err: "downloaded file size: {downloaded_file_size} ' \
                        f'while file size in path: {file_size_in_path}'
            raise errors.ResourceInvalid(path=path, msg=error_msg)

    def geturl(self, path, purpose="download"):
        logger.debug(f'geturl path: {path}')
        if purpose != "download":
            raise errors.NoURL(path, purpose)

        try:
            current_context = self.get_context_by_path(path)
            self.verify_upload_complete(path, context=current_context)
            s3_url = current_context.raw_obj.getFileUrl(self.basespace.base_api)
        except errors.ResourceInvalid as e:
            raise e
        except Exception as e:
            logging.exception(f"Failed to get URL for path: {path}")
            raise errors.NoURL(path, purpose, msg=str(e))

        return s3_url

    def verify_upload_complete(self, path, context=None):
        is_complete = context.raw_obj.UploadStatus == 'complete'
        if not is_complete:
            raise errors.ResourceInvalid(path=path, msg=f"File has not been uploaded yet. status: {context.raw_obj.UploadStatus}")

    def get_context_by_path(self, path):
        _path = self.validatepath(path)

        try:
            _key = self._path_to_key(_path)
            info = self.getinfo(path)
        except Exception:
            raise errors.ResourceNotFound(path)
        else:
            if info.is_dir:
                raise errors.FileExpected(path)

        return self._get_context_by_key(_key)

    def makedir(self, path, permissions=None, recreate=False):
        raise errors.ResourceReadOnly

    def remove(self, path):
        raise errors.ResourceReadOnly

    def removedir(self, path):
        raise errors.ResourceReadOnly

    def setinfo(self, path, info):
        raise errors.ResourceReadOnly

    def upload(self, path, file, chunk_size=None, **options):
        raise errors.ResourceReadOnly
