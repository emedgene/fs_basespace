# coding: utf-8
"""Defines the BASESPACEFSOpener."""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ['BASESPACEFSOpener']

from fs.opener import Opener
from fs.opener.errors import OpenerError

from ._basespacefs import BASESPACEFS


class BASESPACEFSOpener(Opener):
    protocols = ['basespace']

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        client_secret, _, access_token = parse_result.password.partition(":")

        try:
            basespace_fs = BASESPACEFS(
                dir_path=parse_result.path or "/",
                client_id=parse_result.username,
                client_secret=client_secret,
                access_token=access_token,
                basespace_server=parse_result.resource
            )
        except ValueError as v:
            raise OpenerError(f'Could not open file system with given path. Reason: {v}')

        return basespace_fs
