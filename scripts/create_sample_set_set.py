"""Script to add sample_set entity (upsert) to a sample_set_set in a Terra workspace.

Usage:
    > python3 create_sample_set_set.py -w WORKSPACE_NAME -p BILLING_PROJECT -f TSV_FILE -t $(gcloud auth print-identity-token)"""

import argparse
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials


def get_access_token():
    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def call_batch_upsert_api(request_body_list, workspace_name, workspace_project):
    """Call Rawls batchUpsert API to create and/or insert entities into a *_set_set."""

    # for each membership group - call API
    for request in request_body_list:
        # get set_set name (group)
        set_set_name = request.split('"')[3]

        # rawls request URL for batchUpsert
        uri = f"https://rawls.dsde-prod.broadinstitute.org/api/workspaces/{workspace_name}/{workspace_project}/entities/batchUpsert"

        # Get access token and and add to headers for requests.
        headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}
        # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"

        response = requests.post(uri, headers=headers, data=request)
        status_code = response.status_code

        # print success or fail message
        if status_code == 204:
            print(f"Successfully created and/or updated group with given entities for: {set_set_name}")

        else:
            print(f"Failed to create and/or update group with given entities for: {set_set_name}")
            print("WARNING: Please see full response for error.")
            print(response.text)


def create_sample_set_set(tsv_file):
    """Parse tsv load file by membership group (sample_set_set) to be created in final data table and call API."""

    # read tsv into pandas dataframe and get the list of unique membership groups
    df_tsv = pd.read_csv(tsv_file, sep="\t")

    # check for required column name for membership table .tsv
    if "membership:sample_set_set_id" not in df_tsv.columns:
        print("Input .tsv does not have required column header for a membership table."
              "\n" + "example header: 'membership:sample_set_set_id'.")
        exit(1)
    else:
        membership_groups = df_tsv["membership:sample_set_set_id"].unique()

    # templates for request body components
    template_req_body = '[{"name":"SET_SET_ID","entityType":"sample_set_set",' + \
                        '"operations":[{"op":"RemoveAttribute","attributeName":"sample_sets"},' + \
                        '{"op":"CreateAttributeEntityReferenceList","attributeListName":"sample_sets"}, ADD_LIST_MEMBER]}]'

    template_ADD_LIST_MEM = '{"op":"AddListMember","attributeListName":"sample_sets",' + \
                            '"newMember":{"entityName":"SAMPLE_SET_ID", "entityType":"sample_set"}},'

    # initialize empty list for multiple group request
    all_group_request_body = []

    # for each individual membership group
    for group in membership_groups:
        # create df specific to membership group
        df_single_group = df_tsv.loc[df_tsv["membership:sample_set_set_id"] == group]
        # from membership specific df, get just IDs of entity
        sample_ids = df_single_group["sample_set"]

        list_members = ''''''
        for sample in sample_ids:
            list_members += (template_ADD_LIST_MEM.replace("SAMPLE_SET_ID", sample))

        # remove trailing comma from final item in list and remove newlines
        list_members = list_members[:-1]

        # insert membership group name and list of entitites to add to membership group to create final request body
        single_group_request_body = template_req_body.replace("SET_SET_ID", group).replace("ADD_LIST_MEMBER", list_members)

        # append single membership group response body to list for all group response bodies
        all_group_request_body.append(single_group_request_body)

    return(all_group_request_body)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-w', '--workspace_name', required=True, help='name of workspace in which to make changes')
    parser.add_argument('-p', '--workspace_project', required=True, help='billing project (namespace) of workspace in which to make changes')
    parser.add_argument('-f', '--tsv_file', required=True, help='.tsv file formatted in load format to Terra UI')
    parser.add_argument('-t', '--access_token', type=str, required=True, help='user access token - use $(gcloud auth print-identity-token)')
    args = parser.parse_args()

    # function to create response body
    request_body_list = create_sample_set_set(args.tsv_file)

    # function accepts request body and posts to batchUpsert
    call_batch_upsert_api(request_body_list, args.workspace_name, args.workspace_project)