from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BiosamplesApi import BiosamplesApi
from BaseSpacePy.api.DatasetsApi import DatasetsApi


class BasespaceApiFactory():

    def __init__(self, client_id, client_secret, basespace_server, access_token):
        self.base_api = BaseSpaceAPI(client_id,
                                     client_secret,
                                     basespace_server,
                                     AccessToken=access_token)
        v2_server = self.get_v2_server(basespace_server)
        self.biosamples_api = BiosamplesApi(access_token=self.base_api.apiClient.apiKey,
                                            api_server_and_version=v2_server)
        self.datasets_api = DatasetsApi(access_token=self.base_api.apiClient.apiKey,
                                        api_server_and_version=v2_server)

    def get_v2_server(self, basespace_server: str) -> str:
        if basespace_server.endswith('/'):
            basespace_server = basespace_server[:-1]
        return f'{basespace_server}/v2'

