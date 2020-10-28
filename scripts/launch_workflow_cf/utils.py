# -*- coding: utf-8 -*-
"""Cloud Function Utilities."""

import os
import json
import requests

from google.cloud.secretmanager_v1 import SecretManagerServiceClient
# from google.oauth2 import service_account
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials

from firecloud import api as fapi


def get_access_token():
    """Takes a path to a service account's json credentials file and return an access token with scopes."""
    scopes = ['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email']

    if 'GCP_PROJECT' in os.environ:  # inside the CF
        service_account_key = SecretManager().get_secret("service_account_key")
        json_acct_info = json.loads(service_account_key)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(json_acct_info, scopes=scopes)
    else:  # running locally
        credentials = GoogleCredentials.get_application_default()
        credentials = credentials.create_scoped(scopes)

    #####

    # credentials = service_account.Credentials.from_service_account_info(json_acct_info).with_scopes(scopes)
    # print(dir(credentials))
    # access_token = credentials.get_access_token().access_token

    # return access_token

    #######

    # METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/'
    # METADATA_HEADERS = {'Metadata-Flavor': 'Google'}
    # SERVICE_ACCOUNT = 'default'

    # url = '{}instance/service-accounts/{}/token?scopes='.format(
    #     METADATA_URL, SERVICE_ACCOUNT, ','.join(scopes))
    
    # print(url)

    # # Request an access token from the metadata server.
    # r = requests.get(url, headers=METADATA_HEADERS)
    # r.raise_for_status()

    # # Extract the access token from the response.
    # access_token = r.json()['access_token']

    # return access_token

    # credentials = GoogleCredentials.get_application_default()
    # credentials = credentials.create_scoped(scopes)
    # access_token = credentials.get_access_token().access_token


    ## original code
    
    
    # from_json_keyfile_name(
    #     json_credentials, scopes=scopes)

    return credentials.get_access_token().access_token


### Firecloud API calls

def get_workspace_config(namespace, workspace, cnamespace, config, headers):
    uri = f"https://api.firecloud.org/api/workspaces/{namespace}/{workspace}/method_configs/{cnamespace}/{config}"
    return requests.get(uri, headers=headers)

def update_workspace_config(namespace, workspace, cnamespace, config, body, headers):
    uri = f"https://api.firecloud.org/api/workspaces/{namespace}/{workspace}/method_configs/{cnamespace}/{config}"
    return requests.post(uri, json=body, headers=headers)

def create_submission(wnamespace, workspace, cnamespace, config, headers,
                      entity=None, etype=None, expression=None, use_callcache=True):
    uri = f"https://api.firecloud.org/api/workspaces/{wnamespace}/{workspace}/submissions"
    body = {
        "methodConfigurationNamespace" : cnamespace,
        "methodConfigurationName" : config,
         "useCallCache" : use_callcache
    }
    if etype:
        body['entityType'] = etype
    if entity:
        body['entityName'] = entity
    if expression:
        body['expression'] = expression
    return requests.post(uri, json=body, headers=headers)



class SecretManager:
    """SecretManager class."""

    def __init__(self, project=None):
        """Initialize a class instance."""
        if project is None:
            project = os.environ['GCP_PROJECT']
        # set the project - defaults to current project
        self.project = project

        # create a secret manager service client
        self.client = SecretManagerServiceClient()

    def get_secret(self, secret_name, version="latest"):
        """Return the decoded payload of a secret version.

        Arguments:
            secret_name {string} -- The name of the secret to be retrieved.
            version {string} -- Version of the secret to be retrieved. Default: "latest".

        Returns:
            string -- Decoded secret.

        """
        # generate the path to the key
        # secret_path = projects/{project}/secrets/{secret_name}/versions/{version}
        secret_path = self.client.secret_version_path(self.project, secret_name, version)

        # retrieve the secret from the secret manager api
        response = self.client.access_secret_version(secret_path)

        # return the decoded payload data of the secret version
        return response.payload.data.decode("utf-8")
