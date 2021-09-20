from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import threading
import itertools
import six

from fs import errors
from fs import ResourceType
from fs import tools
from fs.base import FS
from fs.mode import Mode
from fs.info import Info
from fs.path import normpath
from fs.path import relpath
from smart_open.http import SeekableBufferedInputBase

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

from .basespace_context import FileContext
from .basespace_context import CategoryContext
from .basespace_context import get_last_direct_context
from .basespace_context import get_context_by_key


__all__ = ["BASESPACEFS"]
_BASESPACE_DEFAULT_SERVER = "https://api.basespace.illumina.com/"


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


@six.python_2_unicode_compatible
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

        super(BASESPACEFS, self).__init__()

    @property
    def basespace(self):
        if not hasattr(self._tlocal, "basespace"):
            self._tlocal.basespace = BaseSpaceAPI(self.client_id,
                                                  self.client_secret,
                                                  self.basespace_server,
                                                  AccessToken=self.access_token)
        return self._tlocal.basespace

    def __repr__(self):
        return _make_repr(
            self.__class__.__name__,
            self._prefix,
            client_id=(self.client_id, None),
            client_secret=(self.client_secret, None),
            access_token=(self.access_token, None),
        )

    def __str__(self):
        return six.text_type("<basespace '{}'>".format(self._prefix))

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

    def _get_context_by_key(self, key):
        return get_context_by_key(self.basespace, key)

    def getinfo(self, path, namespaces=None):
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        current_context = self._get_context_by_key(_key)
        info_dict = self._info_from_object(current_context, namespaces)

        return Info(info_dict)

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
                "created": raw_obj.DateCreated.timestamp() if raw_obj.DateCreated else None
            }
            if not is_dir:
                details_info["size"] = raw_obj.Size
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
            path,  # type: Text
            namespaces=None,  # type: Optional[Collection[Text]]
            page=None,  # type: Optional[Tuple[int, int]]
    ):
        # type: (...) -> Iterator[Info]
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        info = (
            Info(self._info_from_object(entity, namespaces=namespaces))
            for entity in self._listdir_entities(_key)
        )
        iter_info = iter(info)
        if page is not None:
            start, end = page
            iter_info = itertools.islice(iter_info, start, end)
        return iter_info

    def _listdir_entities(self, key):
        destination = self._get_context_by_key(key)
        return [entry for entry in destination.list(self.basespace)]

    def listdir(self, path):
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        entities_list = self._listdir_entities(_key)
        return sorted([entry.get_id() for entry in entities_list])

    def makedir(self, path, permissions=None, recreate=False):
        raise errors.ResourceReadOnly

    def remove(self, path):
        raise errors.ResourceReadOnly

    def removedir(self, path):
        raise errors.ResourceReadOnly

    def setinfo(self, path, info):
        raise errors.ResourceReadOnly

    def openbin(self, path, mode="r", buffering=-1, **options):
        _mode = Mode(mode)
        if _mode.create:
            raise errors.ResourceReadOnly

        _mode.validate_bin()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        info = None
        try:
            info = self.getinfo(path)
        except errors.ResourceNotFound:
            pass
        else:
            if info.is_dir:
                raise errors.FileExpected(path)

        current_context = self._get_context_by_key(_key)
        s3_url = current_context.raw_obj.getFileUrl(self.basespace)
        return SeekableBufferedInputBase(s3_url, mode)

    def download(self, path, file, chunk_size=None, **options):
        _path = self.validatepath(path)

        with self.openbin(_path, "rb") as basespace_f:
            tools.copy_file_data(basespace_f, file)

    def upload(self, path, file, chunk_size=None, **options):
        raise errors.ResourceReadOnly
