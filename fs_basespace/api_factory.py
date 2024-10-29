from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.api.BiosamplesApi import BiosamplesApi
from BaseSpacePy.api.DatasetsApi import DatasetsApi
import bssh_sdk_2

class BasespaceApiFactory():

    def __init__(self, client_id, client_secret, basespace_server, access_token):
        self.base_api = BaseSpaceAPI(client_id,
                                     client_secret,
                                     basespace_server,
                                     AccessToken=access_token)

        # api SDK-V2 configuration
        v2_configuration = bssh_sdk_2.Configuration()
        v2_configuration.access_token = access_token

        self.v2 = bssh_sdk_2.BasespaceApi(bssh_sdk_2.ApiClient(v2_configuration))
