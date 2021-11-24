# coding: utf-8

"""
    Illumina BaseSpace Platform integration with pyfilesystem2
"""

import os
import unittest

import fs
import vcr
from fs import errors
from fs.errors import ResourceNotFound
from fs.opener.errors import OpenerError

ROOT_PATH = '/'

# Emedgene - MOCK Credentials
CLIENT_KEY = "YYYYY7c5106e4b4b956e128d1d1XXXXX"
CLIENT_SECRET = "YYYYY85e8e9b4248b9b396e8c23XXXXX"
APP_TOKEN = "YYYYY0f27f224388b11f3b193eeXXXXX"
SERVER_AND_VERSION = 'https://api.basespace.illumina.com/v2'

EMEDGENE_PROJECT_ID = 86591915
EMEDGENE_BIOSAMPLE_ID = 104555093
EMEDGENE_DATASET_ID = 'ds.ac82a306af3847f2b53ecb695bc22400'

FILE_1_ID = 11710715826
FILE_1_SIZE_IN_BYTES = 48526491
FILE_1_NAME = 'Myeloid-RNA-Brain-Rep1_S1_L001_R1_001.fastq.gz'

FILE_2_ID = 11710715827
FILE_2_SIZE_IN_BYTES = 50934554
FILE_2_NAME = 'Myeloid-RNA-Brain-Rep1_S1_L001_R2_001.fastq.gz'


#
# [
#     {
#         "Id": "11710715826",
#         "Href": "https://api.basespace.illumina.com/v2/files/11710715826",
#         "HrefContent": "https://api.basespace.illumina.com/v2/files/11710715826/content",
#         "Name": "Myeloid-RNA-Brain-Rep1_S1_L001_R1_001.fastq.gz",
#         "ContentType": "application/x-gzip",
#         "Size": 48526491,
#         "Path": "Myeloid-RNA-Brain-Rep1_S1_L001_R1_001.fastq.gz",
#         "IsArchived": false,
#         "DateCreated": "2018-07-19T23:50:10.0000000Z",
#         "DateModified": "2018-07-19T23:50:10.0000000Z",
#         "ETag": "3a1f3ea201911c15cf166a58ecc783d0-6",
#         "IdAsLong": 11710715826
#     },
#     {
#         "Id": "11710715827",
#         "Href": "https://api.basespace.illumina.com/v2/files/11710715827",
#         "HrefContent": "https://api.basespace.illumina.com/v2/files/11710715827/content",
#         "Name": "Myeloid-RNA-Brain-Rep1_S1_L001_R2_001.fastq.gz",
#         "ContentType": "application/x-gzip",
#         "Size": 50934554,
#         "Path": "Myeloid-RNA-Brain-Rep1_S1_L001_R2_001.fastq.gz",
#         "IsArchived": false,
#         "DateCreated": "2018-07-19T23:50:10.0000000Z",
#         "DateModified": "2018-07-19T23:50:10.0000000Z",
#         "ETag": "b270e3ed6754e7c66be4f20ce13f0a98-7",
#         "IdAsLong": 11710715827
#     }


# CLIENT_ID = "eab50ad2-5b58-MOCK-96e8-bb4a7a1904a4"
# CLIENT_SECRET = "3vjMiJa9WVMOCKgY.05P-_RrCAsBCP~M-a"
# TENANT_ID = "6578252a-6a23-MOCK-88b9-999999999999"
# CONTAINER_NAME = 'test-container'
# ACCOUNT_NAME = 'emedgeneblobfstests'
# URL = f'https://{ACCOUNT_NAME}.blob.core.windows.net'
# ACCOUNT_URL = f'{URL}/{CONTAINER_NAME}'


class TestBaseSpace(unittest.TestCase):
    connection_template = '{scheme}://{client_key}:{client_secret}:{app_token}@{server}!/'
    scheme = 'basespace'
    cassette_lib_dir = os.path.join(os.path.dirname(__file__), 'fixtures/cassettes')

    def _get_conn_str(self, client_key=CLIENT_KEY, client_secret=CLIENT_SECRET, app_token=APP_TOKEN,
                      server=SERVER_AND_VERSION):
        return self.connection_template.format(scheme=self.scheme,
                                               client_key=client_key,
                                               client_secret=client_secret,
                                               app_token=app_token,
                                               server=server)

    def _init_default_fs(self):
        basespace_fs = fs.open_fs(self._get_conn_str())
        self.assertIsNotNone(basespace_fs)
        return basespace_fs

    # open_fs
    def test_open_fs_default(self):
        basespace_fs = fs.open_fs(self._get_conn_str())
        self.assertIsNotNone(basespace_fs)
        self.assertEqual(basespace_fs.client_id, CLIENT_KEY)
        self.assertEqual(basespace_fs.client_secret, CLIENT_SECRET)
        self.assertEqual(basespace_fs.access_token, APP_TOKEN)
        self.assertEqual(basespace_fs.basespace_server, SERVER_AND_VERSION)

    def test_open_fs_empty_client_key(self):
        with self.assertRaises(OpenerError):
            fs.open_fs(self._get_conn_str(client_key=''))

    def test_open_fs_empty_client_secret(self):
        with self.assertRaises(OpenerError):
            fs.open_fs(self._get_conn_str(client_secret=''))

    def test_open_fs_empty_app_token(self):
        with self.assertRaises(OpenerError):
            fs.open_fs(self._get_conn_str(app_token=''))

    # download
    @vcr.use_cassette('download/download_file_1.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_existing_sequenced_file_1(self):
        # prepare
        file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                    f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/{FILE_1_ID}'
        expected_file_size = FILE_1_SIZE_IN_BYTES
        out_file_name = 'my_downloaded_binary_file'

        # init
        basespace_fs = self._init_default_fs()

        # act
        with open(out_file_name, 'wb') as write_file:
            basespace_fs.download(file_name, write_file)

        # assert
        self.assertIsNotNone(write_file)
        with open(out_file_name, "rb") as binary_file:
            data = binary_file.read()
        self.assertEqual(len(data), expected_file_size)

        # cleanup
        os.remove(out_file_name)

    @vcr.use_cassette('download/download_file_2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_existing_sequenced_file_2(self):
        # prepare
        file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                    f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/{FILE_2_ID}'
        expected_file_size = FILE_2_SIZE_IN_BYTES
        out_file_name = 'my_downloaded_binary_file'

        # init
        basespace_fs = self._init_default_fs()

        # act
        with open(out_file_name, 'wb') as write_file:
            basespace_fs.download(file_name, write_file)

        # assert
        self.assertIsNotNone(write_file)
        with open(out_file_name, "rb") as binary_file:
            data = binary_file.read()
        self.assertEqual(len(data), expected_file_size)

        # cleanup
        os.remove(out_file_name)

    @vcr.use_cassette('download/download_non_existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_non_existing_file(self):
        # prepare
        expected_size = 0

        # init
        basespace_fs = self._init_default_fs()

        # act
        out_file_name = 'my_downloaded_no_such_file'
        no_such_file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                            f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/1111111'
        with open(out_file_name, 'wb') as write_file:
            basespace_fs.download(no_such_file_name, write_file)

        # assert
        self.assertIsNotNone(write_file)
        with open(out_file_name, "rb") as binary_file:
            data = binary_file.read()
        self.assertEqual(len(data), expected_size)

        # cleanup
        os.remove(out_file_name)

    @vcr.use_cassette('download/download_an_existing_folder.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_an_existing_folder(self):
        # prepare
        expected_size = 0

        # init
        basespace_fs = self._init_default_fs()

        # act
        folder_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets/'
        out_file_name = 'my_downloaded_no_such_folder'
        with open(out_file_name, 'wb') as write_file:
            basespace_fs.download(folder_name, write_file)

        # assert
        self.assertIsNotNone(write_file)
        with open(out_file_name, "rb") as binary_file:
            data = binary_file.read()
        self.assertEqual(len(data), expected_size)

        # cleanup
        os.remove(out_file_name)

    # getinfo
    @vcr.use_cassette('getinfo/existing_file1.yaml', cassette_library_dir=cassette_lib_dir)
    def test_getinfo_existing_file1(self):
        # prepare

        # init
        basespace_fs = self._init_default_fs()

        # act
        file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                    f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/{FILE_1_ID}'
        info = basespace_fs.getinfo(file_name)

        # assert
        self.assertIsNotNone(info)
        self.assertEqual(info.name, str(FILE_1_ID))
        self.assertFalse(info.is_dir)
        self.assertTrue(info.is_file)

    @vcr.use_cassette('getinfo/existing_dir.yaml', cassette_library_dir=cassette_lib_dir)
    def test_getinfo_existing_dir(self):
        # prepare

        # init
        basespace_fs = self._init_default_fs()

        # act
        folder_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets/'
        info = basespace_fs.getinfo(folder_name)

        # assert
        self.assertIsNotNone(info)
        self.assertEqual(info.name, 'datasets')
        self.assertTrue(info.is_dir)

    @vcr.use_cassette('getinfo/root_dir.yaml', cassette_library_dir=cassette_lib_dir)
    def test_getinfo_root_dir(self):
        # prepare
        folder_name = ROOT_PATH

        # init
        basespace_fs = self._init_default_fs()

        # act and assert
        with self.assertRaises(ResourceNotFound):
            basespace_fs.getinfo(folder_name)

        folder_name = ''
        # act and assert
        with self.assertRaises(ResourceNotFound):
            basespace_fs.getinfo(folder_name)

    @vcr.use_cassette('getinfo/projects_dir.yaml', cassette_library_dir=cassette_lib_dir)
    def test_getinfo_projects_dir(self):
        # prepare
        folder_name = "projects"

        # init
        basespace_fs = self._init_default_fs()

        # act
        info = basespace_fs.getinfo(folder_name)

        # assert
        self.assertIsNotNone(info)
        self.assertEqual(info.name, folder_name)
        self.assertTrue(info.is_dir)

    @vcr.use_cassette('getinfo/non_existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    def test_getinfo_non_existing_file(self):
        # prepare
        no_such_file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                            f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/1111111'

        # init
        basespace_fs = self._init_default_fs()

        # act and assert
        with self.assertRaises(ResourceNotFound):
            basespace_fs.getinfo(no_such_file_name)

    @vcr.use_cassette('getinfo/non_existing_dir.yaml', cassette_library_dir=cassette_lib_dir)
    def test_getinfo_non_existing_dir(self):
        # prepare
        no_such_folder_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasetsssss/'

        # init
        basespace_fs = self._init_default_fs()

        # act and assert
        with self.assertRaises(ResourceNotFound):
            basespace_fs.getinfo(no_such_folder_name)

    # listdir
    @vcr.use_cassette('listdir/existing_dir_datasets.yaml', cassette_library_dir=cassette_lib_dir)
    def test_listdir_existing_dir_datasets(self):
        # prepare
        expected_list = [EMEDGENE_DATASET_ID]

        # init
        basespace_fs = self._init_default_fs()

        # act
        existing_folder = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets/'
        datasets_list = basespace_fs.listdir(existing_folder)

        # assert
        self.assertIsNotNone(datasets_list)
        self.assertListEqual(datasets_list, expected_list)

    @vcr.use_cassette('listdir/existing_dir_biosamples.yaml', cassette_library_dir=cassette_lib_dir)
    def test_listdir_existing_dir_biosampless(self):
        # prepare
        expected_list = ['104555093', '104555094', '104555095', '104555096', '104555097', '104555098', '104555099',
                         '104555100', '104555101', '104555102']

        # init
        basespace_fs = self._init_default_fs()

        # act
        existing_folder = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/'
        biosamples_list = basespace_fs.listdir(existing_folder)

        # assert
        self.assertIsNotNone(biosamples_list)
        self.assertListEqual(biosamples_list, expected_list)

    @vcr.use_cassette('listdir/existing_dir_projects.yaml', cassette_library_dir=cassette_lib_dir)
    def test_listdir_existing_dir_projects(self):
        # prepare
        expected_list = [str(EMEDGENE_PROJECT_ID)]

        # init
        basespace_fs = self._init_default_fs()

        # act
        existing_folder = f'/projects/'
        projects_list = basespace_fs.listdir(existing_folder)

        # assert
        self.assertIsNotNone(projects_list)
        self.assertListEqual(projects_list, expected_list)

    def test_listdir_non_existing_dir(self):
        # prepare
        no_such_folder_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamplesXXX/'

        # init
        basespace_fs = self._init_default_fs()

        # act and assert
        with self.assertRaises(errors.DirectoryExpected):
            basespace_fs.listdir(no_such_folder_name)

    @vcr.use_cassette('listdir/existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    def test_listdir_existing_file(self):
        # prepare
        file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                    f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/{FILE_1_ID}'

        # init
        basespace_fs = self._init_default_fs()

        # act & verify
        with self.assertRaises(errors.DirectoryExpected):
            basespace_fs.listdir(file_name)

    # openbin
    @vcr.use_cassette('openbin/existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    def test_openbin_existing_file(self):
        # prepare
        expected_size = FILE_1_SIZE_IN_BYTES
        full_file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                         f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/{FILE_1_ID}'

        # init
        basespace_fs = fs.open_fs(self._get_conn_str())
        self.assertIsNotNone(basespace_fs)

        # act
        remote_binary_file = basespace_fs.openbin(full_file_name, mode='rb')

        # assert
        self.assertIsNotNone(remote_binary_file)
        self.assertTrue(remote_binary_file.seekable())
        self.assertEqual(remote_binary_file.content_length, expected_size)
        self.assertEqual(remote_binary_file.tell(), 0)

    @vcr.use_cassette('openbin/non_existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    def test_openbin_non_existing_file(self):
        # prepare
        no_such_file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                            f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/1111111'

        # init
        basespace_fs = self._init_default_fs()

        # act & assert
        with self.assertRaises(errors.ResourceNotFound):
            basespace_fs.openbin(no_such_file_name, mode='rb')

    #
    #
    # @vcr.use_cassette('openbin/existing_folder.yaml', cassette_library_dir=cassette_lib_dir)
    # def test_openbin_existing_folder(self):
    #     # prepare
    #     path = BLOB_BINARY_FOLDER
    #
    #     # init
    #     blob_fs = self._init_default_fs()
    #
    #     # act & assert
    #     with self.assertRaises(errors.ResourceNotFound):
    #         blob_fs.openbin(path, mode='rb')
    #
    # @vcr.use_cassette('openbin/non_existing_folder.yaml', cassette_library_dir=cassette_lib_dir)
    # def test_openbin_non_existing_folder(self):
    #     # prepare
    #     path = '/binary-file'
    #
    #     # init
    #     blob_fs = self._init_default_fs()
    #
    #     # act & assert
    #     with self.assertRaises(errors.ResourceNotFound):
    #         blob_fs.openbin(path, mode='rb')
    #
    # @vcr.use_cassette('openbin/existing_file_1.yaml', cassette_library_dir=cassette_lib_dir)
    # def test_openbin_existing_file_1(self):
    #     # prepare
    #     expected_size = 67851
    #     blob_name = f'{BLOB_VISUALIZATION_FOLDER}{BLOB_FOLDER_5}{BLOB_FOLDER_4}/19-384.emg-norm_v1.0.5.vcf.gz'
    #
    #     # init
    #     blob_fs = self._init_default_fs()
    #
    #     # act
    #     remote_binary_file = blob_fs.openbin(blob_name, mode='rb')
    #
    #     # assert
    #     self.assertTrue(remote_binary_file.seekable())
    #     self.assertIsNotNone(remote_binary_file)
    #     self.assertEqual(remote_binary_file.content_length, expected_size)
    #     self.assertEqual(remote_binary_file.tell(), 0)
    #
    # @vcr.use_cassette('openbin/existing_file_with_base_path_and_empty_blob_path.yaml',
    #                   cassette_library_dir=cassette_lib_dir)
    # def test_openbin_existing_file_with_base_path_and_empty_blob_path(self):
    #     # prepare
    #     blob_name = ''
    #
    #     # init
    #     blob_fs = fs.open_fs(self._get_conn_str(blob=BLOB_BINARY_FOLDER))
    #     self.assertIsNotNone(blob_fs)
    #
    #     # act & assert
    #     with self.assertRaises(errors.ResourceNotFound):
    #         blob_fs.openbin(blob_name, mode='rb')
    #
    # # # geturl
    # # @vcr.use_cassette('geturl/geturl.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_geturl_default(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     blob = BLOB_FILE_1
    # #     blob_url = blob_fs.geturl(blob)
    # #
    # #     # validate url structure
    # #     self.assertIsNotNone(blob_url)
    # #     self.assertTrue(f'{CONTAINER_NAME}{BLOB_FILE_1}' in blob_url)
    # #     self.assertTrue(blob_url.startswith(f'{ACCOUNT_URL}'))
    # #
    # #     # validate auth params
    # #     self.assertTrue('se=' in blob_url)  # signedExpiry
    # #     self.assertTrue('sp=' in blob_url)  # signedPermissions
    # #     self.assertTrue('sv=' in blob_url)  # signedVersion
    # #     self.assertTrue('sr=' in blob_url)  # signedResource
    # #     self.assertTrue('skoid=' in blob_url)  # signedObjectId
    # #     self.assertTrue('sktid=' in blob_url)  # signedTenantId
    # #     self.assertTrue('skt=' in blob_url)  # signedKeyStartTime
    # #     self.assertTrue('ske=' in blob_url)  # signedKeyExpiryTime
    # #     self.assertTrue('sks=' in blob_url)  # signedKeyService
    # #     self.assertTrue('skv=' in blob_url)  # signedkeyversion
    # #     self.assertTrue('sig=' in blob_url)  # signature
    # #
    # # @vcr.use_cassette('geturl/with_base_path.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_geturl_with_base_path(self):
    # #     # prepare
    # #     base_blob_folder = BLOB_VISUALIZATION_FOLDER
    # #     blob = f'{BLOB_FOLDER_5}{BLOB_FOLDER_4}/19-384.emg-norm_v1.0.5.vcf.gz'
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob=base_blob_folder))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act
    # #     blob_url = blob_fs.geturl(blob)
    # #
    # #     # validate url structure
    # #     self.assertIsNotNone(blob_url)
    # #     self.assertTrue(f'{CONTAINER_NAME}{BLOB_VISUALIZATION_FOLDER}{blob}' in blob_url)
    # #     self.assertTrue(blob_url.startswith(f'{ACCOUNT_URL}'))
    # #
    # #     # validate auth params
    # #     self.assertTrue('se=' in blob_url)  # signedExpiry
    # #     self.assertTrue('sp=' in blob_url)  # signedPermissions
    # #     self.assertTrue('sv=' in blob_url)  # signedVersion
    # #     self.assertTrue('sr=' in blob_url)  # signedResource
    # #     self.assertTrue('skoid=' in blob_url)  # signedObjectId
    # #     self.assertTrue('sktid=' in blob_url)  # signedTenantId
    # #     self.assertTrue('skt=' in blob_url)  # signedKeyStartTime
    # #     self.assertTrue('ske=' in blob_url)  # signedKeyExpiryTime
    # #     self.assertTrue('sks=' in blob_url)  # signedKeyService
    # #     self.assertTrue('skv=' in blob_url)  # signedkeyversion
    # #     self.assertTrue('sig=' in blob_url)  # signature
    # #
    # # @vcr.use_cassette('geturl/test_geturl_with_base_path_and_empty_blob_path.yaml',
    # #                   cassette_library_dir=cassette_lib_dir)
    # # def test_geturl_with_base_path_and_empty_blob_path(self):
    # #     # prepare
    # #     base_blob_folder = BLOB_VISUALIZATION_FOLDER
    # #     blob = ''
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob=base_blob_folder))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.NoURL):
    # #         blob_fs.geturl(blob)
    # #
    # # @vcr.use_cassette('geturl/empty_blob.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_geturl_empty_blob(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     blob = ''
    # #     with self.assertRaises(errors.NoURL):
    # #         blob_fs.geturl(blob)
    # #
    # #     blob = None
    # #     with self.assertRaises(errors.NoURL):
    # #         blob_fs.geturl(blob)
    # #
    # # @vcr.use_cassette('geturl/geturl_non_existing_blob.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_geturl_non_existing_blob(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     no_blob = '/non-existing-blob/no-blob/no-blob-at-all'
    # #     with self.assertRaises(errors.NoURL):
    # #         blob_fs.geturl(no_blob)
    # #
    # # @vcr.use_cassette('geturl/geturl_geturl_folder_only_blob.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_geturl_folder_only_blob(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # run & assert
    # #     folder_blob = '/folder-123'
    # #     with self.assertRaises(errors.NoURL):
    # #         blob_fs.geturl(folder_blob)
    # #
    # # # isdir
    # # @vcr.use_cassette('isdir/isdir.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isdir(self):
    # #     # prepare
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # validate
    # #     blob = f'{BLOB_3_FOLDERS_2_FILES_FOLDER}{BLOB_FOLDER_2}'
    # #     is_dir = blob_fs.isdir(blob)
    # #
    # #     # assert
    # #     self.assertTrue(is_dir)
    # #
    # # @vcr.use_cassette('isdir/with_base.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isdir_with_base(self):
    # #     # prepare
    # #     base_blob = f'{BLOB_VISUALIZATION_FOLDER}{BLOB_FOLDER_5}'
    # #     blob = BLOB_FOLDER_4
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob=base_blob))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act
    # #     is_dir = blob_fs.isdir(blob)
    # #
    # #     # assert
    # #     self.assertTrue(is_dir)
    # #
    # # @vcr.use_cassette('isdir/with_base_and_empty_blob_path.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isdir_with_base_and_empty_blob_path(self):
    # #     # prepare
    # #     base_blob = f'{BLOB_VISUALIZATION_FOLDER}{BLOB_FOLDER_5}'
    # #     blob = ''
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob=base_blob))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act
    # #     is_dir = blob_fs.isdir(blob)
    # #
    # #     # assert
    # #     self.assertTrue(is_dir)
    # #
    # # @vcr.use_cassette('isdir/root_dir.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isdir_root_dir(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     blob = ROOT_PATH
    # #     is_dir = blob_fs.isdir(blob)
    # #     # assert
    # #     self.assertTrue(is_dir)
    # #
    # #     # act
    # #     blob = BLOB_ROOT
    # #     is_dir = blob_fs.isdir(blob)
    # #     # assert
    # #     self.assertTrue(is_dir)
    # #
    # # @vcr.use_cassette('isdir/an_existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isdir_an_existing_file(self):
    # #     # prepare
    # #     blob = f'{BLOB_3_FOLDERS_2_FILES_FOLDER}/{BLOB_FILE_3}'
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     is_dir = blob_fs.isdir(blob)
    # #
    # #     # assert
    # #     self.assertFalse(is_dir)
    # #
    # # @vcr.use_cassette('isdir/non_existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isdir_non_existing_file(self):
    # #     # prepare
    # #     blob = f'{BLOB_3_FOLDERS_2_FILES_FOLDER}/{BLOB_FILE_3}-non-exist-blob'
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     is_dir = blob_fs.isdir(blob)
    # #
    # #     # assert
    # #     self.assertFalse(is_dir)
    # #
    # # # isfile
    # # @vcr.use_cassette('isfile/isfile.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isfile_existing_file(self):
    # #     # prepare
    # #     blob = f'{BLOB_3_FOLDERS_2_FILES_FOLDER}/{BLOB_FILE_2}'
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     is_file = blob_fs.isfile(blob)
    # #
    # #     # assert
    # #     self.assertTrue(is_file)
    # #
    # # @vcr.use_cassette('isfile/non_existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isfile_non_existing_file(self):
    # #     # prepare
    # #     blob = f'{BLOB_3_FOLDERS_2_FILES_FOLDER}/{BLOB_FILE_2}.non-existing'
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     is_file = blob_fs.isfile(blob)
    # #
    # #     # assert
    # #     self.assertFalse(is_file)
    # #
    # # @vcr.use_cassette('isfile/existing_file_with_base_path.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isfile_existing_file_with_base_path(self):
    # #     # prepare
    # #     base_blob = BLOB_3_FOLDERS_2_FILES_FOLDER
    # #     blob = BLOB_FILE_2
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob=base_blob))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act
    # #     is_file = blob_fs.isfile(blob)
    # #
    # #     # assert
    # #     self.assertTrue(is_file)
    # #
    # # @vcr.use_cassette('isfile/non_existing_file_with_base_path.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_isfile_non_existing_file_with_base_path(self):
    # #     # prepare
    # #     base_blob = BLOB_3_FOLDERS_2_FILES_FOLDER
    # #     blob = f'{BLOB_FILE_2}.non-existing'
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob=base_blob))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act
    # #     is_file = blob_fs.isfile(blob)
    # #
    # #     # assert
    # #     self.assertFalse(is_file)
    # #
    # # def test_isfile_non_existing_file_with_base_path_and_empty_blob_path(self):
    # #     # prepare
    # #     base_blob = BLOB_3_FOLDERS_2_FILES_FOLDER
    # #     blob = ''
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob=base_blob))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act
    # #     is_file = blob_fs.isfile(blob)
    # #
    # #     # assert
    # #     self.assertFalse(is_file)
    # #
    # # def test_isfile_root_and_default_dir(self):
    # #     # prepare
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     blob = ROOT_PATH
    # #     is_file = blob_fs.isfile(blob)
    # #     self.assertFalse(is_file)
    # #
    # #     blob = BLOB_ROOT
    # #     is_file = blob_fs.isfile(blob)
    # #     self.assertFalse(is_file)
    # #
    # #
    # # # scandir
    # # @vcr.use_cassette('scandir/scandir.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_scandir_root_folder(self):
    # #     # prepare
    # #     expected_list = ROOT_BLOB_DETAILED_LIST
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     blob_path = ROOT_PATH
    # #     resource_list = blob_fs.scandir(blob_path)
    # #
    # #     # assert
    # #     resources = []
    # #     for index, fs_resource in enumerate(resource_list):
    # #         resource = {
    # #             "name": fs_resource.name,
    # #             "directory": fs_resource.is_dir
    # #         }
    # #         alias = fs_resource.get('basic', 'alias')
    # #         if alias:
    # #             resource['alias'] = alias
    # #         resources.append(resource)
    # #
    # #     self.assertListEqual(resources, expected_list)
    # #
    # # @vcr.use_cassette('scandir/non_existing_folder.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_scandir_non_existing_folder(self):
    # #     # prepare
    # #     blob_path = 'non-existing-folder'
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceNotFound):
    # #         resource_list = blob_fs.scandir(blob_path)
    # #         for _, _ in enumerate(resource_list):
    # #             print("nothing")
    # #
    # # @vcr.use_cassette('scandir/existing_blob_file.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_scandir_with_existing_blob_file(self):
    # #     # prepare
    # #     blob_path = BLOB_FILE_1
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceNotFound):
    # #         resource_list = blob_fs.scandir(blob_path)
    # #         for _, _ in enumerate(resource_list):
    # #             print("nothing")
    # #
    # # @vcr.use_cassette('scandir/existing_folder.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_scandir_existing_folder(self):
    # #     # prepare
    # #     blob_path = BLOB_3_FOLDERS_2_FILES_FOLDER
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     resource_list = blob_fs.scandir(blob_path)
    # #
    # #     # assert
    # #     resources = []
    # #     folder_count = 3
    # #     file_count = 2
    # #     for index, fs_resource in enumerate(resource_list):
    # #         if fs_resource.is_dir:
    # #             folder_count -= 1
    # #         else:
    # #             file_count -= 1
    # #         print(fs_resource.name)
    # #         resource = {
    # #             "name": fs_resource.name,
    # #             "directory": fs_resource.is_dir
    # #         }
    # #         alias = fs_resource.get('basic', 'alias')
    # #         if alias:
    # #             resource['alias'] = alias
    # #         resources.append(resource)
    # #
    # #     self.assertEqual(folder_count, 0)
    # #     self.assertEqual(file_count, 0)
    # #
    # # @vcr.use_cassette('scandir/existing_folder_with_base_path.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_scandir_existing_folder_with_base_path(self):
    # #     # prepare
    # #     blob_path = BLOB_FOLDER_5
    # #     base_path = BLOB_VISUALIZATION_FOLDER
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob=base_path))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act
    # #     resource_list = blob_fs.scandir(blob_path)
    # #
    # #     # assert
    # #     resources = []
    # #     folder_count = 1
    # #     file_count = 2
    # #     for index, fs_resource in enumerate(resource_list):
    # #         if fs_resource.is_dir:
    # #             folder_count -= 1
    # #         else:
    # #             file_count -= 1
    # #         print(fs_resource.name)
    # #         resource = {
    # #             "name": fs_resource.name,
    # #             "directory": fs_resource.is_dir
    # #         }
    # #         alias = fs_resource.get('basic', 'alias')
    # #         if alias:
    # #             resource['alias'] = alias
    # #         resources.append(resource)
    # #
    # #     self.assertEqual(folder_count, 0)
    # #     self.assertEqual(file_count, 0)
    # #
    # # # scan dir pagination
    # # @vcr.use_cassette('scandir/scandir_pagination.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_scandir_pagination_folder(self):
    # #     # prepare
    # #     offset = 0
    # #     size = 50
    # #     has_more = True
    # #     # there are 311 files and page size is 50
    # #     expected_pagination_reminder = 11
    # #     blob_path = '/pagination-200/311'
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     while has_more:
    # #         resource_list = blob_fs.scandir(blob_path, page=(offset, offset + size))
    # #         offset += size
    # #         resources = []
    # #         for index, fs_resource in enumerate(resource_list):
    # #             resource = {
    # #                 "name": fs_resource.name,
    # #                 "directory": fs_resource.is_dir
    # #             }
    # #             alias = fs_resource.get('basic', 'alias')
    # #             if alias:
    # #                 resource['alias'] = alias
    # #             resources.append(resource)
    # #
    # #         for resource in resources:
    # #             print(f"resource: name: {resource['name']} directory: {resource['directory']}")
    # #             if 'size' in resource.keys():
    # #                 print(f"resource: size: {resource['size']}")
    # #
    # #         has_more = len(resources) == size
    # #         if not has_more:
    # #             self.assertEqual(len(resources), expected_pagination_reminder)
    # #
    # # @vcr.use_cassette('scandir/existing_folder_with_base_path_and_empty_blob_path.yaml',
    # #                   cassette_library_dir=cassette_lib_dir)
    # # def test_scandir_existing_folder_with_base_path_and_empty_blob_path(self):
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob='visualization'))
    # #     self.assertIsNotNone(blob_fs)
    # #     blob_path = ''
    # #     resource_list = blob_fs.scandir(blob_path)
    # #     resources = []
    # #     folder_count = 2
    # #     file_count = 3
    # #     for index, fs_resource in enumerate(resource_list):
    # #         if fs_resource.is_dir:
    # #             folder_count -= 1
    # #         else:
    # #             file_count -= 1
    # #         print(fs_resource.name)
    # #         resource = {
    # #             "name": fs_resource.name,
    # #             "directory": fs_resource.is_dir
    # #         }
    # #         alias = fs_resource.get('basic', 'alias')
    # #         if alias:
    # #             resource['alias'] = alias
    # #         resources.append(resource)
    # #     self.assertEqual(folder_count, 0)
    # #     self.assertEqual(file_count, 0)
    #
    # #
    # #
    # # # readbytes
    # # @vcr.use_cassette('readbytes/readbytes.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_readbytes_existing_file(self):
    # #     # prepare
    # #     expected_size = 8530
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act
    # #     blob = f'{BLOB_3_FOLDERS_2_FILES_FOLDER}/{BLOB_FILE_2}'
    # #     bytes = blob_fs.readbytes(blob)
    # #
    # #     # assert
    # #     self.assertIsNotNone(bytes)
    # #     self.assertEqual(len(bytes), expected_size)
    # #
    # # @vcr.use_cassette('readbytes/non_existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_readbytes_non_existing_file(self):
    # #     # prepare
    # #     blob = f'{BLOB_3_FOLDERS_2_FILES_FOLDER}/{BLOB_FILE_2}-non-exist'
    # #
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceNotFound):
    # #         blob_fs.readbytes(blob)
    # #
    # # @vcr.use_cassette('readbytes/existing_file_with_base_path.yaml', cassette_library_dir=cassette_lib_dir)
    # # def test_readbytes_existing_file_with_base_path(self):
    # #     # prepare
    # #     expected_size = 8530
    # #     blob = BLOB_FILE_2
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob='/3folders-2files'))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act
    # #     bytes = blob_fs.readbytes(blob)
    # #
    # #     # assert
    # #     self.assertIsNotNone(bytes)
    # #     self.assertEqual(len(bytes), expected_size)
    # #
    # # @vcr.use_cassette('readbytes/existing_file_with_base_path_and_empty_blob_path.yaml',
    # #                   cassette_library_dir=cassette_lib_dir)
    # # def test_readbytes_existing_file_with_base_path_and_empty_blob_path(self):
    # #     # prepare
    # #     blob = ''
    # #
    # #     # init
    # #     blob_fs = fs.open_fs(self._get_conn_str(blob='/3folders-2files'))
    # #     self.assertIsNotNone(blob_fs)
    # #
    # #     # act & assert
    # #     bytes = None
    # #     with self.assertRaises(errors.ResourceNotFound):
    # #         bytes = blob_fs.readbytes(blob)
    # #
    # #     self.assertIsNone(bytes)
    # #
    # # # read only methods
    # # def test_removedir(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceReadOnly):
    # #         blob_fs.removedir(BLOB_ROOT)
    # #
    # # def test_setinfo(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceReadOnly):
    # #         blob_fs.setinfo(BLOB_FILE_1, None)
    # #
    # # def test_writebytes(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceReadOnly):
    # #         blob_fs.writebytes('', None)
    # #
    # # def test_upload(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceReadOnly):
    # #         blob_fs.upload('', None)
    # #
    # # def test_copy(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceReadOnly):
    # #         blob_fs.copy(ROOT_PATH, None)
    # #
    # # def test_move(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceReadOnly):
    # #         blob_fs.move(ROOT_PATH, None)
    # #
    # # def test_makedir(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceReadOnly):
    # #         blob_fs.makedir(ROOT_PATH, None)
    # #
    # # def test_remove(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #
    # #     # act & assert
    # #     with self.assertRaises(errors.ResourceReadOnly):
    # #         blob_fs.remove(ROOT_PATH)
    # #
    # # # internal methods
    # # def test_1(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #     blob = "123"
    # #     self.assertFalse(blob_fs._is_dir_element(blob))
    # #
    # # def test_2(self):
    # #     # init
    # #     blob_fs = self._init_default_fs()
    # #     blob = BlobPrefix(prefix="some prefix")
    # #     self.assertTrue(blob_fs._is_dir_element(blob))


if __name__ == '__main__':
    unittest.main()
