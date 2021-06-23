import argparse
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
        print(f"WARNING: Failed to upload entities.")
        print(response.text)
        return

    # entities upsert success
    print(f"Successfully uploaded entities." + "\n")


def write_request_json(request, filename_prefix):
    """Create output file with json request."""

    save_name = f"{filename_prefix}_batch_upsert_request.json"
    with open(save_name, "w") as f:
        f.write(request)


def convert_string_to_list(input_string):
    """Convert a given string into an array compatible with data model tsvs for array attributes."""

    # remove single & double quotes, remove spaces, remove [ ], separate remaining string on commas (resulting in a list)
    output_list = str(input_string).replace("'", '').replace('"', '').replace(" ", "").strip('[]').split(",")

    return output_list


def create_upsert_request(tsv, array_attr_cols=None):
    """Generate the request body for batchUpsert API."""

    # check tsv format: data model load tsv requirement "entity:table_name_id" or "membership:table_name_id" -> else exit
    entity_type_col_name = tsv.columns[0]                               # entity:entity_name_id
    entity_type = entity_type_col_name.rsplit("_", 1)[0].split(":")[1]  # entity_name

    if not entity_type_col_name.startswith(("entity:", "membership:")):
        print("Invalid tsv. The .tsv does not start with column entity:[table_name]_id or membership:[table_name]_id. Please correct and try again.")
        return

    # replace the "entity:col_name_id" with just "col_name" in df
    # if not replaced, "attributeName" in the template_make_single_attr becomes "entity:entity_name_id" instead of just entity_name
    # when the API request is made, its read as multiple columns with the "entity" prefix which is illegal
    # this is specific just to the first column where the format is required for terra load tsv files
    tsv.rename(columns={entity_type_col_name: entity_type}, inplace=True)

    # templates
    # for request body skeleton
    template_req_body = """{"name":"VAR_ENTITY_ID", "entityType":"VAR_ENTITY_TYPE", "operations":[OPERATIONS_LIST]}"""

    # for operations - create/update: list attribute, add list member to attribute if list, single attribute and value
    template_make_list_attr = '{"op":"CreateAttributeValueList","attributeName":"VAR_ATTRIBUTE_LIST_NAME"},'
    template_add_list_member = '{"op":"AddListMember","attributeListName":"VAR_ATTRIBUTE_LIST_NAME", "newMember":"VAR_LIST_MEMBER"},'
    template_make_single_attr = '{"op":"AddUpdateAttribute","attributeName":"VAR_ATTRIBUTE_NAME", "addUpdateAttribute":"VAR_ATTRIBUTE_MEMBER"},'

    # initiate string to capture all operation requests
    all_attributes_request = []

    for index, row in tsv.iterrows():
        single_attribute_request = ''''''
        # get the entity_id (row id - must be unique)
        entity_id = str(row[0])

        # if array columns/attributes
        if array_attr_cols:
            # for each array column
            for col in array_attr_cols:
                # add column name to template for a single attribute
                single_attribute_request += template_make_list_attr.replace("VAR_ATTRIBUTE_LIST_NAME", col)
                # convert string value -> back into an array: [foo,bar] (str) --> ['foo', 'bar'] (list)
                attr_values = convert_string_to_list(row[col])
                # for each item in array, add row attribute value to the above created list attribute
                for val in attr_values:
                    single_attribute_request += template_add_list_member.replace("VAR_LIST_MEMBER", val).replace("VAR_ATTRIBUTE_LIST_NAME", col)

            # get single attribute list based on which of the full tsv columns are array attributes
            single_attr_cols = list(set(list(tsv.columns)) - set(array_attr_cols))
        # if no array columns/attributes
        else:
            single_attr_cols = list(tsv.columns)

        # if there are single attribute columns - cases where all columns are arrays, single_attr_cols would be empty
        if single_attr_cols:
            # for each column that is not an array
            for col in single_attr_cols:
                # get value in col from df
                attr_value = str(row[col])
                # add the request for attribute to list
                single_attribute_request += template_make_single_attr.replace("VAR_ATTRIBUTE_MEMBER", attr_value).replace("VAR_ATTRIBUTE_NAME", col)

        # remove trailing comma from the last request template
        single_attribute_request = single_attribute_request[:-1]

        # put entity_type and entity_id in request body template
        final_single_attribute_request = template_req_body.replace("VAR_ENTITY_ID", entity_id).replace("VAR_ENTITY_TYPE", entity_type)
        # put operations list into request body template
        final_single_attribute_request = final_single_attribute_request.replace("OPERATIONS_LIST", single_attribute_request)

        # add the single workspace request to largest list of individual workspace requests
        all_attributes_request.append(final_single_attribute_request)

    # remove quotes around elements of the list but keep the brackets - using sep() or join() get rid of the brackets
    # ['{}', '{}', '{}'] (api fails) --> [{}, {}, {}] (api succeeds)
    all_attributes_request_formatted = '[%s]' % ','.join(all_attributes_request)

    return all_attributes_request_formatted


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
