# coding: utf-8
# coding: utf-8

"""
    Illumina BaseSpace Platform integration with pyfilesystem2
"""

import os
import unittest

import fs
import vcr
from fs.errors import ResourceNotFound, FileExpected
from fs.errors import NoURL
from fs.errors import DirectoryExpected
from fs.opener.errors import OpenerError

ROOT_PATH = '/'

# Emedgene - MOCK Credentials
CLIENT_KEY = "YYYYY7c5106e4b4b956e128d1d1XXXXX"
CLIENT_SECRET = "YYYYY85e8e9b4248b9b396e8c23XXXXX"
APP_TOKEN = "YYYYY0f27f224388b11f3b193eeXXXXX"
BASESPACE_DEFAULT_SERVER = "https://api.basespace.illumina.com/"


EMEDGENE_PROJECT_ID = 86591915
EMEDGENE_PROJECT_NAME = 'MiSeq: Myeloid RNA Panel (Brain and SeraSeq Samples)'
EMEDGENE_BIOSAMPLE_ID = 104555093
EMEDGENE_DATASET_ID = 'ds.ac82a306af3847f2b53ecb695bc22400'

FILE_1_ID = 11710715826
FILE_1_SIZE_IN_BYTES = 48526491
FILE_1_NAME = 'Myeloid-RNA-Brain-Rep1_S1_L001_R1_001.fastq.gz'

FILE_2_ID = 11710715827
FILE_2_SIZE_IN_BYTES = 50934554
FILE_2_NAME = 'Myeloid-RNA-Brain-Rep1_S1_L001_R2_001.fastq.gz'


class TestBaseSpace(unittest.TestCase):
    connection_template = '{scheme}://{client_key}:{client_secret}:{app_token}@{server}!/'
    scheme = 'basespace'
    cassette_lib_dir = os.path.join(os.path.dirname(__file__), 'fixtures/cassettes')

    def _get_conn_str(self, client_key=CLIENT_KEY, client_secret=CLIENT_SECRET, app_token=APP_TOKEN,
                      server=BASESPACE_DEFAULT_SERVER):
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
        self.assertEqual(basespace_fs.basespace_server, BASESPACE_DEFAULT_SERVER)

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
    @vcr.use_cassette('download/download_file_11.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_existing_sequenced_file_1(self):
        # prepare
        file_name = '/projects/86591915/appresults/137682553/files/11761995736'
        expected_file_size = 1247
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

    @vcr.use_cassette('download/download_file_22.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_existing_sequenced_file_2(self):
        # prepare
        file_name = '/projects/86591915/appresults/137682553/files/11761995733'
        expected_file_size = 41809
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

    @vcr.use_cassette('download/download_file.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_existing_file(self):
        # prepare
        file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets/{EMEDGENE_DATASET_ID}/sequenced files/{FILE_1_ID}'
        expected_file_size = 48526491
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

    @vcr.use_cassette('download/download_non_existing_file_1.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_non_existing_file(self):
        # prepare

        # init
        basespace_fs = self._init_default_fs()

        # act
        out_file_name = 'my_downloaded_no_such_file'
        no_such_file_name = '/projects/385613228/appresults/313508279/files/11111111111'
        with self.assertRaises(NoURL):
            with open(out_file_name, 'wb') as write_file:
                basespace_fs.download(no_such_file_name, write_file)

    @vcr.use_cassette('download/download_an_existing_folder_1.yaml', cassette_library_dir=cassette_lib_dir)
    def test_download_an_existing_folder(self):
        # prepare

        # init
        basespace_fs = self._init_default_fs()

        # act
        folder_name = '/projects/86591915/appresults/137682553/files/'
        out_file_name = 'my_downloaded_existing_folder'
        with self.assertRaises(FileExpected):
            with open(out_file_name, 'wb') as write_file:
                basespace_fs.download(folder_name, write_file)

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
        folder_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples'
        info = basespace_fs.getinfo(folder_name)

        # assert
        self.assertIsNotNone(info)
        self.assertEqual(info.name, 'biosamples')
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
    @vcr.use_cassette('listdir/existing_dir_datasets_v2.yaml', cassette_library_dir=cassette_lib_dir)
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

    @vcr.use_cassette('listdir/existing_dir_samples.yaml', cassette_library_dir=cassette_lib_dir)
    def test_listdir_existing_dir_samples(self):
        # init
        basespace_fs = self._init_default_fs()

        # act
        existing_folder = '/projects/86591915/samples/155127035'
        samples_list = basespace_fs.listdir(existing_folder)

        # assert
        self.assertIsNotNone(samples_list)


    @vcr.use_cassette('c', cassette_library_dir=cassette_lib_dir)
    def test_listdir_existing_dir_biosamples(self):
        # prepare
        expected_list = ['104555093', '104555094', '104555095', '104555096', '104555097', '104555098', '104555099',
                         '104555100', '104555101', '104555102', '104555103', '104555104', '104555105', '104555106',
                         '104555107', '104555108', '104555109', '104555110', '104555111', '104555112', '104555113',
                         '104555114', '104555115', '104555116']

        # init
        basespace_fs = self._init_default_fs()

        # act
        folder_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples'
        biosamples_list = basespace_fs.listdir(folder_name)

        # assert
        self.assertIsNotNone(biosamples_list)
        self.assertListEqual(biosamples_list, expected_list)

    @vcr.use_cassette('listdir/existing_dir_projects.yaml', cassette_library_dir=cassette_lib_dir)
    def test_listdir_existing_dir_projects(self):
        # prepare
        expected_list = ['238837599', '257330073', '312651341', '372438067', '394318963', '48078043', '86591915']
        print(expected_list)

        # init
        basespace_fs = self._init_default_fs()

        # act
        existing_folder = '/projects/'
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
        with self.assertRaises(DirectoryExpected):
            basespace_fs.listdir(no_such_folder_name)

    @vcr.use_cassette('listdir/existing_file.yaml', cassette_library_dir=cassette_lib_dir)
    def test_listdir_existing_file(self):
        # prepare
        file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/' \
                    f'datasets/{EMEDGENE_DATASET_ID}/sequenced files/{FILE_1_ID}'

        # init
        basespace_fs = self._init_default_fs()

        # act & verify
        with self.assertRaises(DirectoryExpected):
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

    @vcr.use_cassette('openbin/non_existing_file_1.yaml', cassette_library_dir=cassette_lib_dir)
    def test_openbin_non_existing_file(self):
        # prepare
        no_such_file_name = '/projects/385613228/appresults/313508279/files/11111111111'

        # init
        basespace_fs = self._init_default_fs()

        # act & assert
        with self.assertRaises(NoURL):
            basespace_fs.openbin(no_such_file_name, mode='rb')

    @vcr.use_cassette('openbin/existing_folder_1.yaml', cassette_library_dir=cassette_lib_dir)
    def test_openbin_existing_folder(self):
        # prepare
        folder_name = '/projects/86591915/appresults/137682553/files/'

        # init
        basespace_fs = self._init_default_fs()

        # act & assert
        with self.assertRaises(FileExpected):
            basespace_fs.openbin(folder_name, mode='rb')

    def test_openbin_non_existing_folder(self):
        # prepare
        no_such_folder_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasetsssss/'

        # init
        basespace_fs = self._init_default_fs()

        # act & assert
        with self.assertRaises(NoURL):
            basespace_fs.openbin(no_such_folder_name, mode='rb')

    def test_openbin_empty_filename(self):
        # prepare
        no_such_folder_name = ''

        # init
        basespace_fs = self._init_default_fs()
        self.assertIsNotNone(basespace_fs)

        # act & assert
        with self.assertRaises(NoURL):
            basespace_fs.openbin(no_such_folder_name, mode='rb')

    # scandir
    def test_scandir_root_folder(self):
        # prepare
        expected_list = [{'name': 'projects', 'directory': True, 'alias': 'projects'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        path = ROOT_PATH
        resource_list = basespace_fs.scandir(path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertListEqual(resources, expected_list)

    @vcr.use_cassette('scandir/projects_folder.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_projects_folder(self):
        # prepare
        # expected_list = [{'name': str(EMEDGENE_PROJECT_ID), 'directory': True, 'alias': EMEDGENE_PROJECT_NAME}]
        expected_list = [{'name': '48078043', 'directory': True, 'alias': 'HiSeqX: Nextera DNA Flex (replicates of Coriell Trio Samples)'},
                         {'name': '86591915', 'directory': True, 'alias': 'MiSeq: Myeloid RNA Panel (Brain and SeraSeq Samples)'},
                         {'name': '238837599', 'directory': True, 'alias': 'NextSeq2000: AmpliSeq for Illumina - Focus Panel (Somatic)'},
                         {'name': '257330073', 'directory': True, 'alias': 'NextSeq2000: Illumina DNA Prep with Enrichment - Exome Germline (Twist/RefSeq Panel)'},
                         {'name': '312651341', 'directory': True, 'alias': 'PGx Analysis v1.1.1 Demo Project'},
                         {'name': '372438067', 'directory': True, 'alias': 'NextSeq2000: Zymo Quick-16S Plus NGS Library Prep Kit (V3-V4) on P1 600 cycles kit'},
                         {'name': '394318963', 'directory': True, 'alias': 'Default Project For Biosample'}]
        # init
        basespace_fs = self._init_default_fs()

        # act
        path = 'projects'
        resource_list = basespace_fs.scandir(path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertListEqual(resources, expected_list)

    @vcr.use_cassette('scandir/project_folder.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_project_folder(self):
        # prepare
        expected_list = [{'alias': 'appresults', 'directory': True, 'name': 'appresults'},
                         {'alias': 'samples', 'directory': True, 'name': 'samples'},
                         {'alias': 'biosamples', 'directory': True, 'name': 'biosamples'},
                         {'alias': 'appsessions', 'directory': True, 'name': 'appsessions'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        project_path = f'/projects/{EMEDGENE_PROJECT_ID}'
        resource_list = basespace_fs.scandir(project_path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertListEqual(resources, expected_list)

    @vcr.use_cassette('scandir/project_biosamples_folder_v2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_project_biosamples_folder(self):
        # prepare
        expected_list = [{'name': '104555093', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep1'}, {'name': '104555094', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep1'},
                         {'name': '104555095', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep9'}, {'name': '104555096', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep2'},
                         {'name': '104555097', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep2'}, {'name': '104555098', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep10'},
                         {'name': '104555099', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep3'}, {'name': '104555100', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep3'},
                         {'name': '104555101', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep11'}, {'name': '104555102', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep4'},
                         {'name': '104555103', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep4'}, {'name': '104555104', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep12'},
                         {'name': '104555105', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep5'}, {'name': '104555106', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep5'},
                         {'name': '104555107', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep13'}, {'name': '104555108', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep6'},
                         {'name': '104555109', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep6'}, {'name': '104555110', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep14'},
                         {'name': '104555111', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep7'}, {'name': '104555112', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep7'},
                         {'name': '104555113', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep15'}, {'name': '104555114', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep8'},
                         {'name': '104555115', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep8'}, {'name': '104555116', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep16'}]




        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples'
        resource_list = basespace_fs.scandir(biosamples_path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertGreaterEqual(len(resources), 10)
        self.assertListEqual(resources, expected_list)

    @vcr.use_cassette('scandir/project_appsessions_folder.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_project_appsessions_folder(self):
        # mock init
        basespace_fs = self._init_default_fs()

        # act
        appsessions_path = '/projects/372438067/appsessions'
        resource_list = basespace_fs.scandir(appsessions_path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertGreaterEqual(len(resources), 205)

    @vcr.use_cassette('scandir/project_files_folder.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_project_files_folder(self):
        # mock init
        basespace_fs = self._init_default_fs()

        # act
        folder_name = '/projects/86591915/appresults/137682553/files/'
        resource_list = basespace_fs.scandir(folder_name)


        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertGreaterEqual(len(resources), 9)

    @vcr.use_cassette('scandir/project_samples_folder.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_project_samples_folder(self):
        # mock init
        basespace_fs = self._init_default_fs()

        # act
        folder_name = '/projects/86591915/samples/'
        resource_list = basespace_fs.scandir(folder_name)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertGreaterEqual(len(resources), 24)

    @vcr.use_cassette('scandir/biosample_folder_v2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_biosample_folder(self):
        # prepare
        expected_list = [{'name': 'datasets', 'directory': True, 'alias': 'datasets'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}'
        resource_list = basespace_fs.scandir(biosamples_path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertListEqual(resources, expected_list)

    @vcr.use_cassette('scandir/datasets_folder_v2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_datasets_folder(self):
        # prepare
        expected_list = [
            {'name': 'ds.ac82a306af3847f2b53ecb695bc22400', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep1_L001'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets'
        resource_list = basespace_fs.scandir(biosamples_path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertListEqual(resources, expected_list)

    @vcr.use_cassette('scandir/datasets_folder_v2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_details_datasets_folder(self):
        # prepare
        expected_list = [
            {'name': 'ds.ac82a306af3847f2b53ecb695bc22400', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep1_L001',
             "extras": {"qc_status": "Undefined"}}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets'
        resource_list = basespace_fs.scandir(biosamples_path, namespaces=["details"])

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            if extras := fs_resource.get('details', 'extras'):
                resource['extras'] = extras
            resources.append(resource)

        self.assertListEqual(resources, expected_list)

    @vcr.use_cassette('scandir/appsessions_datasets.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_appsessions_datasets_folder(self):
        # prepare
        expected_list = [{'alias': 'in3529-18V3V4', 'directory': True, 'name': 'ds.a6802fbf9b274ca08a9796b5bcd0e8e2'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/372438067/appsessions/628976393/datasets/'
        resource_list = basespace_fs.scandir(biosamples_path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertListEqual(resources, expected_list)

    def test_scandir_non_existing_folder(self):
        # prepare
        no_such_folder_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasetsssss/'

        # init
        basespace_fs = self._init_default_fs()

        # act & assert
        with self.assertRaises(ResourceNotFound):
            basespace_fs.scandir(no_such_folder_name)

    @vcr.use_cassette('scandir/existing_folder.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_existing_folder(self):
        # prepare
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/'

        # init
        basespace_fs = self._init_default_fs()

        # act
        resource_list = basespace_fs.scandir(biosamples_path)

        # assert
        resources = []
        folder_count = 24
        file_count = 0
        for index, fs_resource in enumerate(resource_list):
            if fs_resource.is_dir:
                folder_count -= 1
            else:
                file_count -= 1
            print(fs_resource.name)
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertEqual(folder_count, 0)
        self.assertEqual(file_count, 0)

    @vcr.use_cassette('scandir/dataset_files_v2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_dataset_files(self):
        # prepare
        expected_file_list = [
            {'name': '11710715826', 'directory': False, 'alias': 'Myeloid-RNA-Brain-Rep1_S1_L001_R1_001.fastq.gz'},
            {'name': '11710715827', 'directory': False, 'alias': 'Myeloid-RNA-Brain-Rep1_S1_L001_R2_001.fastq.gz'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets/{EMEDGENE_DATASET_ID}/sequenced files'
        resource_list = basespace_fs.scandir(biosamples_path)

        # assert
        resources = []
        for index, fs_resource in enumerate(resource_list):
            resource = {
                "name": fs_resource.name,
                "directory": fs_resource.is_dir
            }
            alias = fs_resource.get('basic', 'alias')
            if alias:
                resource['alias'] = alias
            resources.append(resource)

        self.assertListEqual(resources, expected_file_list)

    # geturl
    @vcr.use_cassette('geturl/of_file.yaml', cassette_library_dir=cassette_lib_dir)
    def test_geturl_of_file(self):
        # prepare
        file_name = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets/' \
                    f'{EMEDGENE_DATASET_ID}/sequenced files/{FILE_1_ID}'

        # init
        basespace_fs = fs.open_fs(self._get_conn_str())
        self.assertIsNotNone(basespace_fs)

        # act
        file_url = basespace_fs.geturl(file_name)

        # validate
        self.assertIsNotNone(file_url)
        self.assertTrue(FILE_1_NAME in file_url)
        self.assertTrue("AWSAccessKeyId" in file_url)
        self.assertTrue("Signature" in file_url)
        self.assertTrue("Expires" in file_url)

    def test_geturl_empty_file_name(self):
        # prepare
        file_name = ''

        # init
        basespace_fs = fs.open_fs(self._get_conn_str())
        self.assertIsNotNone(basespace_fs)

        # act
        with self.assertRaises(NoURL):
            basespace_fs.geturl(file_name)

    @vcr.use_cassette('geturl/non_existing_file_1.yaml', cassette_library_dir=cassette_lib_dir)
    def test_geturl_non_existing_file(self):
        # prepare
        no_such_file_name = '/projects/385613228/appresults/313508279/files/11111111111'

        # init
        basespace_fs = fs.open_fs(self._get_conn_str())
        self.assertIsNotNone(basespace_fs)

        # act
        with self.assertRaises(NoURL):
            basespace_fs.geturl(no_such_file_name)

    # PAGINATION
    @vcr.use_cassette('scandir/project_biosamples_folder_pagination_v2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_project_biosamples_folder_pagination(self):
        # prepare
        expected_list = [{'name': '104555093', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep1'},
                         {'name': '104555094', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep1'},
                         {'name': '104555095', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep9'},
                         {'name': '104555096', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep2'},
                         {'name': '104555097', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep2'},
                         {'name': '104555098', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep10'},
                         {'name': '104555099', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep3'},
                         {'name': '104555100', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep3'},
                         {'name': '104555101', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep11'},
                         {'name': '104555102', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep4'},
                         {'name': '104555103', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep4'},
                         {'name': '104555104', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep12'},
                         {'name': '104555105', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep5'},
                         {'name': '104555106', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep5'},
                         {'name': '104555107', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep13'},
                         {'name': '104555108', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep6'},
                         {'name': '104555109', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep6'},
                         {'name': '104555110', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep14'},
                         {'name': '104555111', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep7'},
                         {'name': '104555112', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep7'},
                         {'name': '104555113', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep15'},
                         {'name': '104555114', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep8'},
                         {'name': '104555115', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep8'},
                         {'name': '104555116', 'directory': True, 'alias': 'Myeloid-RNA-SeraSeq-Rep16'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples'
        step = 5
        start = 0
        end = step
        page = (start, end)
        full_resources_list = []
        while True:
            resource_list = basespace_fs.scandir(biosamples_path, page=page)
            # assert
            resources = []
            for index, fs_resource in enumerate(resource_list):
                resource = {
                    "name": fs_resource.name,
                    "directory": fs_resource.is_dir
                }
                alias = fs_resource.get('basic', 'alias')
                if alias:
                    resource['alias'] = alias
                resources.append(resource)

            full_resources_list += resources

            for resource in resources:
                print(
                    f"resource: name: {resource['name']} directory: {resource['directory']}   alias: {resource['alias']}")
                if 'size' in resource.keys():
                    print(f"resource: size: {resource['size']}")
            if len(resources) < step:
                offset, _ = page
                print(f"last ({offset} {offset + len(resources)}) --------------------------------")
                break
            print(f"{page}--------------------------------")
            start = end
            end += step
            page = (start, end)

        self.assertGreaterEqual(len(full_resources_list), len(expected_list))
        self.assertListEqual(full_resources_list, expected_list)

    @vcr.use_cassette('scandir/datasets_folder_pagination_v2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_datasets_folder_pagination(self):
        # prepare
        expected_list = [
            {'name': 'ds.ac82a306af3847f2b53ecb695bc22400', 'directory': True, 'alias': 'Myeloid-RNA-Brain-Rep1_L001'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets'

        step = 5
        start = 0
        size = step
        page = (start, size)
        full_resources_list = []
        while True:
            resource_list = basespace_fs.scandir(biosamples_path, page=page)

            # assert
            resources = []
            for index, fs_resource in enumerate(resource_list):
                resource = {
                    "name": fs_resource.name,
                    "directory": fs_resource.is_dir
                }
                alias = fs_resource.get('basic', 'alias')
                if alias:
                    resource['alias'] = alias
                resources.append(resource)

            full_resources_list += resources

            for resource in resources:
                print(
                    f"resource: name: {resource['name']} directory: {resource['directory']}   alias: {resource['alias']}")
                if 'size' in resource.keys():
                    print(f"resource: size: {resource['size']}")
            if len(resources) < step:
                offset, _ = page
                print(f"last ({offset} {offset + len(resources)}) --------------------------------")
                break
            print(f"{page}--------------------------------")
            start = size
            size += step
            page = (start, size)

        self.assertListEqual(full_resources_list, expected_list)

    @vcr.use_cassette('scandir/dataset_files_pagination_v2.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_dataset_files_pagination(self):
        # prepare
        expected_file_list = [
            {'name': '11710715826', 'directory': False, 'alias': 'Myeloid-RNA-Brain-Rep1_S1_L001_R1_001.fastq.gz'},
            {'name': '11710715827', 'directory': False, 'alias': 'Myeloid-RNA-Brain-Rep1_S1_L001_R2_001.fastq.gz'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        biosamples_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets/{EMEDGENE_DATASET_ID}/sequenced files'

        step = 1
        start = 0
        size = step
        page = (start, size)
        full_resources_list = []
        while True:
            resource_list = basespace_fs.scandir(biosamples_path, page=page)

            # assert
            resources = []
            for index, fs_resource in enumerate(resource_list):
                resource = {
                    "name": fs_resource.name,
                    "directory": fs_resource.is_dir
                }
                alias = fs_resource.get('basic', 'alias')
                if alias:
                    resource['alias'] = alias
                resources.append(resource)

            full_resources_list += resources

            for resource in resources:
                print(
                    f"resource: name: {resource['name']} directory: {resource['directory']}   alias: {resource['alias']}")
                if 'size' in resource.keys():
                    print(f"resource: size: {resource['size']}")
            if len(resources) < step:
                offset, _ = page
                print(f"last ({offset} {offset + len(resources)}) --------------------------------")
                break
            print(f"{page}--------------------------------")
            start = size
            size += step
            page = (start, size)

        self.assertListEqual(full_resources_list, expected_file_list)

    @vcr.use_cassette('scandir/projects_pagination.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_projects_pagination(self):
        # prepare
        expected_file_list = [{'name': '48078043', 'directory': True, 'alias': 'HiSeqX: Nextera DNA Flex (replicates of Coriell Trio Samples)'},
                              {'name': '86591915', 'directory': True, 'alias': 'MiSeq: Myeloid RNA Panel (Brain and SeraSeq Samples)'},
                              {'name': '238837599', 'directory': True, 'alias': 'NextSeq2000: AmpliSeq for Illumina - Focus Panel (Somatic)'},
                              {'name': '257330073', 'directory': True, 'alias': 'NextSeq2000: Illumina DNA Prep with Enrichment - Exome Germline (Twist/RefSeq Panel)'},
                              {'name': '312651341', 'directory': True, 'alias': 'PGx Analysis v1.1.1 Demo Project'},
                              {'name': '372438067', 'directory': True, 'alias': 'NextSeq2000: Zymo Quick-16S Plus NGS Library Prep Kit (V3-V4) on P1 600 cycles kit'},
                              {'name': '394318963', 'directory': True, 'alias': 'Default Project For Biosample'}]

        # init
        basespace_fs = self._init_default_fs()

        # act
        project_path = '/projects/'

        step = 3
        start = 0
        size = step
        page = (start, size)
        full_resources_list = []
        while True:
            resource_list = basespace_fs.scandir(project_path, page=page)

            # assert
            resources = []
            for index, fs_resource in enumerate(resource_list):
                resource = {
                    "name": fs_resource.name,
                    "directory": fs_resource.is_dir
                }
                alias = fs_resource.get('basic', 'alias')
                if alias:
                    resource['alias'] = alias
                resources.append(resource)

            full_resources_list += resources

            for resource in resources:
                print(
                    f"resource: name: {resource['name']} directory: {resource['directory']}   alias: {resource['alias']}")
                if 'size' in resource.keys():
                    print(f"resource: size: {resource['size']}")
            if len(resources) < step:
                offset, _ = page
                print(f"last ({offset} {offset + len(resources)}) --------------------------------")
                break
            print(f"{page}--------------------------------")
            start = size
            size += step
            page = (start, size)
        self.assertListEqual(full_resources_list, expected_file_list)

    @vcr.use_cassette('scandir/appsessions_pagination.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_appsessions_pagination(self):
        # prepare
        # init
        basespace_fs = self._init_default_fs()

        # act
        appsessions_path = '/projects/372438067/appsessions'

        step = 100
        start = 0
        size = step
        page = (start, size)
        full_resources_list = []
        while True:
            resource_list = basespace_fs.scandir(appsessions_path, page=page)

            # assert
            resources = []
            for index, fs_resource in enumerate(resource_list):
                resource = {
                    "name": fs_resource.name,
                    "directory": fs_resource.is_dir
                }
                alias = fs_resource.get('basic', 'alias')
                if alias:
                    resource['alias'] = alias
                resources.append(resource)

            full_resources_list += resources

            for resource in resources:
                print(
                    f"resource: name: {resource['name']} directory: {resource['directory']}   alias: {resource['alias']}")
                if 'size' in resource.keys():
                    print(f"resource: size: {resource['size']}")
            if len(resources) < step:
                offset, _ = page
                print(f"last ({offset} {offset + len(resources)}) --------------------------------")
                break
            print(f"{page}--------------------------------")
            start = size
            size += step
            page = (start, size)
        self.assertIsNotNone(full_resources_list)
        self.assertGreaterEqual(len(full_resources_list), 200)

    @vcr.use_cassette('scandir/appresults_pagination.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_appresults_pagination(self):
        # prepare
        # init
        basespace_fs = self._init_default_fs()

        # act
        appresults_path = '/projects/86591915/appresults'

        step = 55
        start = 0
        size = step
        page = (start, size)
        full_resources_list = []
        while True:
            resource_list = basespace_fs.scandir(appresults_path, page=page)

            # assert
            resources = []
            for index, fs_resource in enumerate(resource_list):
                resource = {
                    "name": fs_resource.name,
                    "directory": fs_resource.is_dir
                }
                alias = fs_resource.get('basic', 'alias')
                if alias:
                    resource['alias'] = alias
                resources.append(resource)

            full_resources_list += resources

            for resource in resources:
                print(
                    f"resource: name: {resource['name']} directory: {resource['directory']}   alias: {resource['alias']}")
                if 'size' in resource.keys():
                    print(f"resource: size: {resource['size']}")
            if len(resources) < step:
                offset, _ = page
                print(f"last ({offset} {offset + len(resources)}) --------------------------------")
                break
            print(f"{page}--------------------------------")
            start = size
            size += step
            page = (start, size)
        self.assertIsNotNone(full_resources_list)
        self.assertGreaterEqual(len(resources), 51)

    @vcr.use_cassette('scandir/sequenced_files_pagination.yaml', cassette_library_dir=cassette_lib_dir)
    def test_scandir_sequenced_files_pagination(self):
        # prepare
        # init
        basespace_fs = self._init_default_fs()

        # act
        sequenced_files_path = f'/projects/{EMEDGENE_PROJECT_ID}/biosamples/{EMEDGENE_BIOSAMPLE_ID}/datasets/{EMEDGENE_DATASET_ID}/sequenced files'

        step = 1
        start = 0
        size = step
        page = (start, size)
        full_resources_list = []
        while True:
            resource_list = basespace_fs.scandir(sequenced_files_path, page=page)

            # assert
            resources = []
            for index, fs_resource in enumerate(resource_list):
                resource = {
                    "name": fs_resource.name,
                    "directory": fs_resource.is_dir
                }
                alias = fs_resource.get('basic', 'alias')
                if alias:
                    resource['alias'] = alias
                resources.append(resource)

            full_resources_list += resources

            for resource in resources:
                print(
                    f"resource: name: {resource['name']} directory: {resource['directory']}   alias: {resource['alias']}")
                if 'size' in resource.keys():
                    print(f"resource: size: {resource['size']}")
            if len(resources) < step:
                offset, _ = page
                print(f"last ({offset} {offset + len(resources)}) --------------------------------")
                break
            print(f"{page}--------------------------------")
            start = size
            size += step
            page = (start, size)
        self.assertIsNotNone(full_resources_list)
        self.assertGreaterEqual(len(full_resources_list), 2)


if __name__ == '__main__':
    unittest.main()
