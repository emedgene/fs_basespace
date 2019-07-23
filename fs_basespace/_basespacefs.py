from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ["BASESPACEFS"]

import threading

from fs import errors
from fs import ResourceType
from fs.base import FS
from fs.mode import Mode
from fs.info import Info
from fs.path import normpath, relpath

import six

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

from .basespace_context import UserContext
from .basespace_context import FileContext
from .basespace_context import CategoryContext


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
        self.basespace_server = basespace_server

        super(BASESPACEFS, self).__init__()

    @property
    def basespace(self):
        if not hasattr(self._tlocal, "basespace"):
            self._tlocal.basespace = BaseSpaceAPI(self.client_id,
                                                  self.client_secret,
                                                  self.basespace_server,
                                                  AccessToken=self.access_token)
        return self._tlocal.basespace

    def get_user(self):
        if not hasattr(self._tlocal, "user"):
            self._tlocal.user = UserContext(self.basespace.getUserById('current'))
        return self._tlocal.user

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

    def _path_to_key(self, path):
        """Converts an fs path to a basespace path."""
        _path = relpath(normpath(path))
        _key = (
            "{}/{}".format(self._prefix, _path).strip("/")
        )
        return _key

    def _get_context_by_key(self, key):
        current_context = self.get_user()
        for tag in key.strip("/").split("/"):
            try:
                current_context = current_context.get(self.basespace, tag)
            except KeyError:
                raise errors.ResourceNotFound
        return current_context

    def getinfo(self, path, namespaces=None):
        self.check()
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        current_context = self._get_context_by_key(_key)
        info_dict = self._info_from_object(current_context, namespaces)

        return Info(info_dict)

    def _info_from_object(self, obj, namespaces):
        """ Make an info dict from a Datalake info() return.

            List of functional namespaces: https://github.com/PyFilesystem/pyfilesystem2/blob/master/fs/info.py
        """
        raw_obj = obj.get_raw()
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
                "created": str(raw_obj.DateCreated)
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

    def listdir(self, path):
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        destination = self._get_context_by_key(_key)
        return sorted([entry.get_id() for entry in destination.list(self.basespace)])

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
        _mode.validate_bin()
        self.check()
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

        if _mode.create:
            raise errors.ResourceReadOnly

        # TODO: return file descriptor here
        raise NotImplementedError("todo: return descriptor for %s" % _key)

    def download(self, path, file, chunk_size=None, **options):
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        current_context = self._get_context_by_key(_key)

        if not isinstance(current_context, FileContext):
            raise errors.FileExpected

        with self._lock:
            # TODO: download file here
            raise NotImplementedError("todo")

    def upload(self, path, file, chunk_size=None, **options):
        raise errors.ResourceReadOnly
