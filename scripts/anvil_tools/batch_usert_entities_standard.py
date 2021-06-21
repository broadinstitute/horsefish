import argparse
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials


# function to get authorization bearer token for requests
def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def call_rawls_batch_upsert(workspace_name, project, request):
    """Post entities to Terra workspace using batchUpsert."""

    # rawls request URL for batchUpsert
    uri = f"https://rawls.dsde-prod.broadinstitute.org/api/workspaces/{project}/{workspace_name}/entities/batchUpsert"

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}

    # capture response from API and parse out status code
    response = requests.post(uri, headers=headers, data=request)
    status_code = response.status_code

    if status_code != 204:  # entities upsert fail
        print(f"WARNING: Failed to upsert entities to {workspace_name}.")
        print(response.text)
        return

    # entities upsert success
    print(f"Successful upsert of entities to {workspace_name}.")


def write_request_json(request):
    """Create output file with json request."""

    save_name = "batch_upsert_request.json"
    with open(save_name, "w") as f:
        f.write(request)


def create_upsert_request(tsv, array_attr_cols=None):
    """Generate the request body for batchUpsert API."""

    # check if the tsv is formatted correctly - exit script if not in right load format
    entity_type_col_name = tsv.columns[0]
    entity_type = entity_type_col_name.rsplit("_", 1)[0].split(":")[1]
    print(f"entity_type_col_name: {entity_type_col_name}")
    print(f"entity_type: {entity_type}")
    if not entity_type_col_name.startswith(("entity:", "membership:")):
        print("Invalid tsv. The .tsv does not start with column entity:[table_name]_id or membership:[table_name]_id. Please correct and try again.")
        return

    # templates for request body components
    template_req_body = '''{"name":"VAR_ENTITY_ID",
                             "entityType":"VAR_ENTITY_TYPE",
                             "operations":[OPERATIONS_LIST]}'''

    template_make_list_attr = '{"op":"CreateAttributeValueList","attributeName":"VAR_ATTRIBUTE_LIST_NAME"},'
    template_add_list_member = '{"op":"AddListMember","attributeListName":"VAR_ATTRIBUTE_LIST_NAME", "newMember":"VAR_LIST_MEMBER"},'
    template_make_single_attr = '{"op":"AddUpdateAttribute","attributeName":"VAR_ATTRIBUTE_NAME", "addUpdateAttribute":"VAR_ATTRIBUTE_MEMBER"},'

    # initiate string to capture all operation requests
    single_attribute_request = ''''''
    all_attributes_request = []

    for index, row in tsv.iterrows():
        # get the entity_id (row id - must be unique)
        entity_id = str(row[0])
        print(f"entity_id: {entity_id}")

        # if there are array attribute columns
        if array_attr_cols:
            print(f"array_attr_cols: {array_attr_cols}")
            # for each column that is an array
            for col in array_attr_cols:
                single_attribute_request += template_make_list_attr.replace("VAR_ATTRIBUTE_LIST_NAME", col)
                # convert "array" from tsv which translates to a string back into an array
                attr_values = str(row[col]).replace('"', '').strip('[]').split(",")
                # print(f"attr_values: {attr_values}")
                for val in attr_values:
                    single_attribute_request += template_add_list_member.replace("VAR_LIST_MEMBER", val).replace("VAR_ATTRIBUTE_LIST_NAME", col)
            # set the column values for the single attribute list based on which of the full tsv columns are array attributes
            single_attr_cols = list(set(list(tsv.columns)) - set(array_attr_cols))
        else:
            single_attr_cols = list(tsv.columns)

        print(f"single attribute reqest post arrays: {single_attribute_request}")

        # if there are single attribute columns
        if single_attr_cols:
            print(f"single_attr_cols: {single_attr_cols}")
            # for each column that is not an array
            for col in single_attr_cols:
                # get value in col from df
                attr_value = str(row[col])
                # print(f"attr_value: {attr_value}")

                # add the request for attribute to list
                single_attribute_request += template_make_single_attr.replace("VAR_ATTRIBUTE_MEMBER", attr_value).replace("VAR_ATTRIBUTE_NAME", col)

        # remove trailing comma from the last request template
        single_attribute_request = single_attribute_request[:-1]
        # print(f"post singe attribute request: {single_attribute_request}")

        # put entity_type and entity_id in request body template
        final_single_attribute_request = template_req_body.replace("VAR_ENTITY_ID", entity_id).replace("VAR_ENTITY_TYPE", entity_type)
        # put operations list into request body template
        final_single_attribute_request = final_single_attribute_request.replace("OPERATIONS_LIST", single_attribute_request)

        all_attributes_request.append(final_single_attribute_request)
    # write out a json of the request body
    # write_request_json(final_request)

    return all_attributes_request


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-w', '--workspace_name', required=True, help='name of workspace in which to make changes')
    parser.add_argument('-p', '--project', required=True, help='billing project (namespace) of workspace in which to make changes')
    parser.add_argument('-t', '--tsv', required=True, help='.tsv file formatted in load format to Terra UI')
    args = parser.parse_args()

    # create request body for batchUpsert
    request = create_upsert_request(args.tsv)
    # call batchUpsert API (rawls)
    call_rawls_batch_upsert(args.workspace_name, args.project, request)
