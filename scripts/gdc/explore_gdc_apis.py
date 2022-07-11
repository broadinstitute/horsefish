# -*- coding: utf-8 -*-
import argparse
import datetime
import pandas as pd
import requests
import json


def read_auth_token(token_file):
    """Read auth token from file."""

    # read contents of auth token file into string
    with open(token_file, "r") as token:
        auth_token = str(token.read().strip())

    return auth_token


def get_gdc_file_metadata(sample_alias, token_file):
    """Get metadata about files from GDC based on sample name."""

    files_endpt = "https://api.gdc.cancer.gov/files"

    # TODO: what additional filters?
    filters = {"op":"=",
               "content":{
                            "field": "submitter_id",
                            "value": f"{sample_alias}"
                            # "value": f"{sample_alias}.*"
                         }
              }

    # files endpoint filed names: https://docs.gdc.cancer.gov/API/Users_Guide/Appendix_A_Available_Fields/#file-fields
    # TODO: which fields are needed as a starting point?
    fields = ["file_name", "submitter_id", "state"]

    params = {"filters": json.dumps(filters),
              "fields": ",".join(fields),
              "format": "TSV",
              "pretty": "TRUE"
              }  # sort etc

    # read contents of auth token file into string
    with open(token_file, "r") as token:
        token_string = str(token.read().strip())
    headers = {"X-Auth-Token": token_string, "Content-Type": "application/json"}

    # call files endpoint and capture response
    files_res = requests.get(files_endpt, params = params, headers=headers)

    # write response to tsv file
    output_filename = f"{sample_alias}_files_response.tsv"
    with open(output_filename, "w") as f:
        f.write(files_res.text)

    print(f"Files endpoint response for {sample_alias} written to output file named {output_filename}.")


def get_submission_status(auth_token):
    """Call submissions endpoint to get status of submission."""

    submissions_endpt = "https://api.gdc.cancer.gov/v0/submission/graphql"

    query = {"query": "{file_name (project_id: \"TCGA-HNSC\") {\r\n    id\r\n    submitter_id\r\n    file_name\r\n    project_id\r\n}\r\n}"}

    headers = {"X-Auth-Token": auth_token, "Content-type": "application/json"}
    response = requests.post(submissions_endpt, headers=headers, json=query)

    output_filename = f"submissions_response.tsv"
    with open(output_filename, "w") as f:
        f.write(response.text)

    print(f"Check output file.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="GDC API to get file metadata.")

    parser.add_argument("-t", "--token-file", required=True, type=str, help="txt file with GDC auth token.")
    parser.add_argument("-s", "--sample-alias", required=True, type=str, help="sample alias to search for")

    args = parser.parse_args()

    # TODO: python script to accept tsv of sample aliases? Or bash script to loop through file and run python script per sample.
    auth_token = read_auth_token(args.token_file)
    get_submission_status(auth_token)

    # get_gdc_file_metadata(args.sample_alias, args.token_file)