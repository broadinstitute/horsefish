"""This script resolves drs uris to gs paths."""

import argparse
import json
from pydoc import resolve
import requests
from pprint import pprint

from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def write_list_to_file(input_list, outfile_name):
    """Write a list of values to a file."""

    with open(outfile_name, "w") as outfile:
        for item in input_list:
            outfile.write(f"{item}\n")


def resolve_drs_uri(drs_uuid):
    """Resolve a drs uri to return the corresponding gs path."""

    uri = f"https://data.terra.bio/ga4gh/drs/v1/objects/{drs_uuid}"

    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*"}

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        return response.text

    # get a list of the available access methods for a drs uri
    avail_access_methods = response.json()["access_methods"]
    # if there is an access method with "type" == "gs", get the gs url (other type options = https)
    gs_url = [access_type["access_url"]["url"] for access_type in avail_access_methods if access_type["type"] == "gs"] # ["gs://"] or []

    # if a gs url is not available, return "NA"
    if not gs_url:
        return "NA"
    
    # if gs url is available, return gs path
    return gs_url[0]


def resolve_drs_to_gs_paths(infile, outfile_name):
    """For each line in file, resolve drs uri to gs path."""

    with open(infile, 'r') as input:
        drs_paths_to_resolve = input.readlines()
    
    gs_paths = []
    for drs in drs_paths_to_resolve:
        # example drs = "drs://data.terra.bio/v1_########-####-####-####-############_########-####-####-####-############"      
        drs_uuid = drs.split("/")[3].strip("\n")  
        gs_url = resolve_drs_uri(drs_uuid)

        gs_paths.append(gs_url)
    
    write_list_to_file(gs_paths, outfile_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--infile', '-f', help="path to file containing newline-delimited list of drs uris to resolve to gs paths")
    parser.add_argument('--outfile', '-o', help="name of output file which will have list of the resolved gs paths")

    args = parser.parse_args()

    resolve_drs_to_gs_paths(args.infile, args.outfile)
