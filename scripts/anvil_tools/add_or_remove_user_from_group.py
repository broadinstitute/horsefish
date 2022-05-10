"""Add/remove user to group with defined acceess level (member or admin) - parsed from input tsv file.

Usage:
    > python3 add_or_remove_user_from_group.py -t TSV_FILE [--delete]"""

import argparse
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def call_removeUserFromGroup_api(group_name, user_email, access_level):
    """DELETE request to the removeUserFromGroup API."""

    # request URL for removeUserFromGroup
    uri = f"https://api.firecloud.org/api/groups/{group_name}/{access_level}/{user_email}"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*"}
    # -H  "accept: */*" -H  "Authorization: Bearer [token]

    # capture response from API and parse out status code
    response = requests.delete(uri, headers=headers)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code == 204:
        print(f"Successfully removed {user_email} from {group_name}@firecloud.org.")

    else:
        print(f"WARNING: Failed to remove {user_email} from {group_name}@firecloud.org.")
        print("Please see full response for error:")
        print(response.text)


def call_addUserToGroup_api(group_name, user_email, access_level):
    """PUT request to the addUserToGroup API."""

    # request URL for addUserToGroup
    uri = f"https://api.firecloud.org/api/groups/{group_name}/{access_level}/{user_email}"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*"}
    # -H  "accept: */*" -H  "Authorization: Bearer [token]

    # capture response from API and parse out status code
    response = requests.put(uri, headers=headers)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code == 204:
        print(f"Successfully added {user_email} to {group_name}@firecloud.org with {access_level} permissions.")

    else:
        print(f"WARNING: Failed to add {user_email} to {group_name}@firecloud.org.")
        print("Please see full response for error:")
        print(response.text)


def add_group_user(tsv, delete):
    """Create individual request per group and user with acceess level listed in tsv file."""

    # read full tsv into dataframe, workspace name = index
    tsv_all = pd.read_csv(tsv, sep="\t")

    # make json request for each workspace and call API
    for row in tsv_all.index:
        # get workspace name from row (Series)
        group_name = tsv_all.loc[row].get(key='group_name')
        access_level = tsv_all.loc[row].get(key='accessLevel')
        user_email = tsv_all.loc[row].get(key='user_email')

        if delete:      # remove user from group
            call_removeUserFromGroup_api(group_name, user_email, access_level)
        else:           # add user to group
            call_addUserToGroup_api(group_name, user_email, access_level)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="add a Terra user to a Terra group")

    parser.add_argument("-t", "--tsv", required=True, type=str, help="tsv file with group, user, and user access levels to add user to group.")
    parser.add_argument('-d', '--delete', required=False, action='store_true', help='set parameter to REMOVE user from group.')
    args = parser.parse_args()

    # call to create request body PER row and make API call to add user to group
    add_group_user(args.tsv, args.delete)
