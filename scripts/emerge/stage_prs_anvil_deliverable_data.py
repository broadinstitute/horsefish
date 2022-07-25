# imports and environment variables
import argparse
import json
import pandas as pd

from firecloud import api as fapi
from google.cloud import storage as gcs

# set which field to use as the PK for data tables (the unique entity id field)
ENTITY_ID_FIELD_NAME = "anvil_poly_sample_id"


def upload_data_table(tsv, workspace, namespace):
    """Upload tsv load file to workspace to create/add to existing data table."""

    upload_response = fapi.upload_entities_tsv(namespace, workspace, tsv, model='flexible')
    status_code = upload_response.status_code

    if status_code != 200:  # api call fails
        raise Exception(f"Uploading entity failed: {upload_response.text}")

    print(upload_response.text)


def make_terra_data_table_tsvs(prs_dictionaries, dest_workspace, dest_namespace):
    """Create the terra data table tsv files for final AnVIL delivery workspace."""

    # convert list of prs dictionaries (each dictionary = 1 prs entity with updated paths to renamed/rehomed files)
    prs_entities_df = pd.DataFrame(prs_dictionaries)

    ## Samples
    # create samples subset df
    anvil_samples_df = prs_entities_df[["emerge_id", ENTITY_ID_FIELD_NAME, "reported_gender"]]
    # rename entity column and make entity:entity_id column first in tsv
    anvil_samples_df = anvil_samples_df.rename(columns={ENTITY_ID_FIELD_NAME: "entity:Sample_id"})
    # create redundant anvil_poly_sample_id column - same value as entity:sample_id
    anvil_samples_df[ENTITY_ID_FIELD_NAME] = anvil_samples_df["entity:Sample_id"]
    first_column = anvil_samples_df.pop("entity:Sample_id")
    anvil_samples_df.insert(0, "entity:Sample_id", first_column)

    # Arrays
    # create arrays subset df
    anvil_arrays_df = prs_entities_df[["red_idat_cloud_path", "green_idat_cloud_path",
                                       "gtc_file", "arrays_variant_calling_detail_metrics_file",
                                       "single_sample_vcf", "single_sample_vcf_index",
                                       "imputed_single_sample_vcf", "imputed_single_sample_vcf_index",
                                       ENTITY_ID_FIELD_NAME]]
    # rename entity column and make entity:entity_id column first in tsv
    anvil_arrays_df = anvil_arrays_df.rename(columns={ENTITY_ID_FIELD_NAME: "entity:Array_id"})
    first_column = anvil_arrays_df.pop("entity:Array_id")
    anvil_arrays_df.insert(0, "entity:Array_id", first_column)

    # Polygenic Risk Scores
    # create prs_scores subset df
    anvil_prs_scores_df = prs_entities_df[["ast_raw", "ast_adjusted", "ast_percentile", "brca_raw", "brca_adjusted",
                                           "brca_percentile", "afib_raw", "afib_adjusted", "afib_percentile",
                                           "chd_raw", "chd_adjusted", "chd_percentile", "ckd_raw", "ckd_adjusted",
                                           "ckd_percentile", "hcl_raw", "hcl_adjusted", "hcl_percentile", "bmi_raw",
                                           "bmi_adjusted", "bmi_percentile", "prca_raw", "prca_adjusted", "prca_percentile",
                                           "t1d_raw", "t1d_adjusted", "t1d_percentile", "t2d_raw", "t2d_adjusted", "t2d_percentile",
                                           ENTITY_ID_FIELD_NAME]]
    # rename entity column and make entity:entity_id column first in tsv
    anvil_prs_scores_df = anvil_prs_scores_df.rename(columns={ENTITY_ID_FIELD_NAME: "entity:Polygenic_Risk_Score_id"})
    first_column = anvil_prs_scores_df.pop("entity:Polygenic_Risk_Score_id")
    anvil_prs_scores_df.insert(0, "entity:Polygenic_Risk_Score_id", first_column)

    # write individual dataframes to tsv files
    anvil_arrays_df.to_csv("arrays.tsv", sep = "\t", index=False)
    anvil_samples_df.to_csv("samples.tsv", sep="\t", index=False)
    anvil_prs_scores_df.to_csv("prs_scores.tsv", sep="\t", index=False)

    # load tsv files to destination workspace
    for tsv in ["arrays.tsv", "samples.tsv", "prs_scores.tsv"]:
        print(f"Loading {tsv} to {dest_workspace}.")
        upload_data_table(tsv, dest_workspace, dest_namespace)


def copy_object(src_bucket_name, src_object_name, dest_bucket_name, dest_object_name):
    """Copies object from one bucket to another with a new name."""

    storage_client = gcs.Client()

    source_bucket = storage_client.bucket(src_bucket_name)
    source_object = source_bucket.blob(src_object_name)
    destination_bucket = storage_client.bucket(dest_bucket_name)

    blob_copy = source_bucket.copy_blob(source_object, destination_bucket, dest_object_name)


def rename_and_rehome_data_files(prs_entities_dicts, dest_bucket, snapshot_id):
    """For columns in the df that represent files, update paths pointing to the final bucket and copy files."""

    # file types and their file extensions to use for renaming files in delivery workspace
    file_extensions = {"red_idat_cloud_path":"_Red.idat", "green_idat_cloud_path":"_Grn.idat",
                      "gtc_file":".gtc", "arrays_variant_calling_detail_metrics_file":".arrays_control_code_summary_metrics",
                      "single_sample_vcf":"_single_sample.vcf.gz", "single_sample_vcf_index":"_single_sample.vcf.gz.tbi",
                      "imputed_single_sample_vcf":"_imputed_single_sample.vcf.gz", "imputed_single_sample_vcf_index":"_imputed_single_sample.vcf.gz.tbi"}

    print(f"Starting renaming and rehoming of PRS samples.")
    for prs_entity in prs_entities_dicts:
        for file_type in file_extensions.keys():
            dest_filename = prs_entity[ENTITY_ID_FIELD_NAME] + file_extensions[file_type]
            dest_filepath = f"gs://{dest_bucket}/{snapshot_id}/{dest_filename}"
            dest_blob = "/".join(dest_filepath.split("/")[3:])

            # copy file from source to destination bucket and rename
            src_bucket = prs_entity[file_type].split("/")[2]
            src_blob = "/".join(prs_entity[file_type].split("/")[3:]) # TDR source filepath

            print(f"Initiate copy of gs://{src_bucket}/{src_blob} to {dest_filepath}")
            copy_object(src_bucket, src_blob, dest_bucket, dest_blob)

            # update dictionary with new paths in destiantion workspace
            prs_entity[file_type] = dest_filepath

    return prs_entities_dicts


def get_single_entity(workspace, namespace, entity_type, entity_id):
    """Get single entity of an entity type."""

    entity_response = fapi.get_entity(namespace, workspace, entity_type, entity_id)
    status_code = entity_response.status_code

    if status_code != 200:  # api call fails
        raise Exception(f"Getting entity information failed: {entity_response.text}")

    return entity_response.json()["attributes"]


def get_prs_entities(workspace, namespace, ids_file):
    """Get dataframe for PRS entities defined from inputs file of entity ids."""

    with open(ids_file) as ids:
        ids_list = ids.readlines()
        ids_list = [entity_id.rstrip() for entity_id in ids_list]

        print(str(len(ids_list)) + " samples to rename and copy to AnVIL delivery workspace.")

    all_prs_entities = [] # list of dictionaries - each item = 1 PRS row

    # for each sample/ID in file (row in Prs Outputs Table)
    for prs_entity_id in ids_list:
        print(f"Gathering data for {prs_entity_id}.")
        # get required information from PRS Outputs Table
        prs_entity_dict = get_single_entity(workspace, namespace, "PrsOutputsTable", prs_entity_id)
        # get snapshot for prs entity/row
        snapshot_id = prs_entity_dict['import:snapshot_id']
        prs_entity_dict.pop('chip_well_barcode', None) # pop chip_well_barcode information

        # get required files + metadata from Arrays Inputs Table
        arrays_inputs_dict = get_single_entity(workspace, namespace, "ArraysInputsTable", prs_entity_id)
        if arrays_inputs_dict['import:snapshot_id'] != snapshot_id:
            raise Exception(f"Snapshot ID does not match for {prs_entity_id} between PrsOutputsTable and ArraysInputsTable.")
        for attribute in ["datarepo_row_id", "import:timestamp", "import:snapshot_id", "chip_well_barcode"]: 
            arrays_inputs_dict.pop(attribute)
        prs_entity_dict.update(arrays_inputs_dict)

        # get required files + metadata from Arrays Outputs Table
        arrays_outputs_dict = get_single_entity(workspace, namespace, "ArraysOutputsTable", prs_entity_id)
        if arrays_outputs_dict['import:snapshot_id'] != snapshot_id:
            raise Exception(f"Snapshot ID does not match for {prs_entity_id} between PrsOutputsTable and ArraysOutputsTable.")
        for attribute in ["datarepo_row_id", "import:timestamp", "import:snapshot_id", "chip_well_barcode_output"]:
            arrays_outputs_dict.pop(attribute)

        # rename for clarity of single sample vcfs (arrays outputs) vs imputed vcfs (imputation output)
        arrays_outputs_dict["single_sample_vcf"] = arrays_outputs_dict.pop("output_vcf")
        arrays_outputs_dict["single_sample_vcf_index"] = arrays_outputs_dict.pop("output_vcf_index")
        prs_entity_dict.update(arrays_outputs_dict)

        # get required files + metadata from Imputation Wide Outputs Table
        imputation_outputs_dict = get_single_entity(workspace, namespace, "ImputationWideOutputsTable", prs_entity_id)
        if imputation_outputs_dict['import:snapshot_id'] != snapshot_id:
            raise Exception(f"Snapshot ID does not match for {prs_entity_id} between PrsOutputsTable and ImputationWideOutputsTable.")
        for attribute in ["datarepo_row_id", "import:timestamp", "import:snapshot_id", "chip_well_barcode"]:
            imputation_outputs_dict.pop(attribute)
        prs_entity_dict.update(imputation_outputs_dict)

        all_prs_entities.append(prs_entity_dict)

    return all_prs_entities, snapshot_id


if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Push Arrays.wdl outputs to TDR dataset.')

    parser.add_argument('-f', '--ids_file', required=True, type=str, help='new line delimited txt file with entity ids to get from table')
    parser.add_argument('-dw', '--dest_workspace', required=True, type=str, help='name of destiantion workspace')
    parser.add_argument('-dn', '--dest_namespace', required=True, type=str, help='namespace/project of destination workspace')
    parser.add_argument('-db', '--dest_bucket', required=True, type=str, help='bucket id (fc-) of destination workspace')
    parser.add_argument('-sw', '--src_workspace', required=True, type=str, help='name of source workspace')
    parser.add_argument('-sn', '--src_namespace', required=True, type=str, help='namespace/project of source workspace')

    args = parser.parse_args()

    prs_entities_list, snapshot_id = get_prs_entities(args.src_workspace, args.src_namespace, args.ids_file)
    prs_entities_rehomed = rename_and_rehome_data_files(prs_entities_list, args.dest_bucket, snapshot_id)
    make_terra_data_table_tsvs(prs_entities_rehomed, args.dest_workspace, args.dest_namespace)
