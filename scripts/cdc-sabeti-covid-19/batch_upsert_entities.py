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


def create_upsert_request(tsv):
    """Generate the request body for batchUpsert API."""

    df_tsv = pd.read_csv(tsv, sep="\t")

    # check if the tsv is formatted correctly - exit script if not in right load format
    entity_type_col_name = df_tsv.columns[0]
    if not entity_type_col_name.startswith("entity:"):
        print("Not a valid tsv. The .tsv does not start with column entity:[table_name]_id. Please correct and try again.")
        return

    # define which columns are array values and which are not
    array_attr_cols = ["assembled_ids", "assemblies_fasta", "cleaned_reads_unaligned_bams",
                       "cleaned_bams_tiny", "demux_commonBarcodes", "demux_metrics", "demux_outlierBarcodes",
                       "failed_annotation_ids", "failed_assembly_ids", "passing_assemblies_fasta",
                       "primer_trimmed_read_count", "primer_trimmed_read_percent", "raw_reads_unaligned_bams",
                       "read_counts_depleted", "read_counts_raw", "submittable_assemblies_fasta", "submittable_ids",
                       "vadr_outputs", "data_tables_out"]

    single_attr_cols = ["assembly_stats_tsv", "cleaned_bam_uris", "genbank_fasta", "genbank_source_table",
                        "gisaid_fasta", "gisaid_meta_tsv", "ivar_trim_stats_html", "ivar_trim_stats_png",
                        "ivar_trim_stats_tsv", "max_ntc_bases", "meta_by_filename_json",
                        "multiqc_report_cleaned", "multiqc_report_raw", "nextclade_all_json", "nextclade_auspice_json",
                        "nextmeta_tsv", "num_assembled", "num_failed_annotation", "num_failed_assembly",
                        "num_read_files", "num_samples", "num_submittable", "picard_metrics_wgs",
                        "run_date", "sequencing_reports", "spikein_counts", "sra_metadata", "submission_xml",
                        "submission_zip", "submit_ready"]

    # templates for request body components
    template_req_body = '''[{"name":"VAR_ENTITY_ID",
                             "entityType":"VAR_ENTITY_TYPE",
                             "operations":[OPERATIONS_LIST]}]'''

    template_make_list_attr = '{"op":"CreateAttributeValueList","attributeName":"VAR_ATTRIBUTE_LIST_NAME"},'
    template_add_list_member = '{"op":"AddListMember","attributeListName":"VAR_ATTRIBUTE_LIST_NAME", "newMember":"VAR_LIST_MEMBER"},'
    template_make_single_attr = '{"op":"AddUpdateAttribute","attributeName":"VAR_ATTRIBUTE_NAME", "addUpdateAttribute":"VAR_ATTRIBUTE_MEMBER"},'

    # initiate string to capture all operation requests
    all_operation_requests = ''''''

    # if there are single attribute columns
    if single_attr_cols:
        # for each column that is not an array
        for col in single_attr_cols:
            # get valuue in col from df
            attr_val = str(df_tsv.iloc[0][col])

            # add the request for attribute to list
            all_operation_requests += template_make_single_attr.replace("VAR_ATTRIBUTE_MEMBER", attr_val).replace("VAR_ATTRIBUTE_NAME", col)

    # if there are array attribute columns
    if array_attr_cols:
        # for each column that is an array
        for col in array_attr_cols:
            all_operation_requests += template_make_list_attr.replace("VAR_ATTRIBUTE_LIST_NAME", col)
            # convert "array" from tsv which translates to a string back into an array
            attr_values = str(df_tsv.iloc[0][col]).replace('"', '').strip('[]').split(",")
            for val in attr_values:
                all_operation_requests += template_add_list_member.replace("VAR_LIST_MEMBER", val).replace("VAR_ATTRIBUTE_LIST_NAME", col)

    # remove trailing comma from the last request template
    all_operation_requests = all_operation_requests[:-1]

    # get the entity_type (table name) and entity_id (row id - must be unique)
    entity_id = str(df_tsv.iloc[0][0])
    entity_type = entity_type_col_name.rsplit("_", 1)[0].split(":")[1]

    # put entity_type and entity_id in request body template
    final_request = template_req_body.replace("VAR_ENTITY_ID", entity_id).replace("VAR_ENTITY_TYPE", entity_type)
    # put operations list into request body template
    final_request = final_request.replace("OPERATIONS_LIST", all_operation_requests)

    # write out a json of the request body
    write_request_json(final_request)

    return final_request


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
