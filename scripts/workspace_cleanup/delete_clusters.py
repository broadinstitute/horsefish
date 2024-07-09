#! /usr/bin/env python
"""Delete all Terra clusters listed in a csv.

Usage: > python3 delete_clusters.py --path PATH_TO_FILE --project PROJECT --token $(gcloud auth print-identity-token)"""

import argparse
import requests
from time import sleep

from oauth2client.client import GoogleCredentials


def get_access_token():
    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def delete_cluster(cluster, project, access_token, retry=0):
    # don't retry too much
    if retry > 3:
        print(f"WARNING: internal errors not resolved by retries. cluster {project}/{cluster} was NOT deleted.")
        print("Exiting.")
        exit(1)

    sleep_time = 5  # seconds to wait between retries

    # try to delete the cluster
    try:
        uri = f"https://notebooks.firecloud.org/api/google/v1/runtimes/{project}/{cluster}?deleteDisk=true"
        # Get access token and and add to headers for requests.
        headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*"}
        #  -H  "accept: */*" -H  "Authorization: Bearer [token]"
        response = requests.delete(uri, headers=headers)
        status_code = response.status_code
        if status_code == 202:
            print(f"cluster {project}/{cluster} was successfully deleted!")
            return 1
        elif status_code == 403:
            print(f"WARNING: Insufficient permissions to delete {project}/{cluster}.")
            return 0
        # elif status_code == 401:
        #     print(f"401: cluster {project}/{cluster} not found. Cool!")
        #     return 1
        elif status_code == 404:
            print(f"404: cluster {project}/{cluster} not found. Cool!")
            return 1
        elif status_code == 500:
            # try again
            incremented_retry = retry + 1
            print(f"Retrying deletion of {project}/{cluster} (attempt {incremented_retry}) after {sleep_time} seconds")
            sleep(sleep_time)
            return delete_cluster(cluster, project, incremented_retry)
        else:
            print(f"Unknown status code: {status_code}. Exiting.")
            exit(1)
    except Exception:
        # try again
        incremented_retry = retry + 1
        print(f"Retrying deletion of {project}/{cluster} (attempt {incremented_retry}) after {sleep_time} seconds")
        sleep(sleep_time)
        return delete_cluster(cluster, project, incremented_retry)


def main(filepath, project, access_token):
    # load csv
    with open(filepath, "r") as infile:
        data = infile.readlines()

    # pull out list of clusters
    cluster_list = [row.rstrip('\n') for row in data if len(row) > 0]

    print(f"Found {len(cluster_list)} clusters to delete.")

    # loop through list of clusters and delete them
    n_deleted = 0
    for cluster in cluster_list:
        success = delete_cluster(cluster, project, access_token)
        n_deleted += success

    print(f"Deleted {n_deleted} out of {len(cluster_list)} clusters.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--path', type=str, required=True, help='path to file containing clusters to delete')
    parser.add_argument('--project', type=str, required=True, help='Terra project in which the clusters reside')
    parser.add_argument('--token', type=str, required=True, help='user access token - use $(gcloud auth print-identity-token)')

    args = parser.parse_args()

    main(args.path, args.project, args.token)