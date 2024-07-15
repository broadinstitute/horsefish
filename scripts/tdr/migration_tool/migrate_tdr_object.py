""" 
Migrates a TDR object to a new TDR dataset as configured by the user.

Usage
    > python3 migrate_tdr_object.py -c PATH_TO_CONFIG_FILE

DEPENDENCIES
    > pip install --upgrade data_repo_client
    > pip install --upgrade google-cloud-storage
    > pip install --upgrade azure-storage-blob

CONFIGURATION
{
    "source": {
        "tdr_object_uuid": "e4634bcb-c52c-4ac8-8759-29ae2e14aaef",
        "tdr_object_type": "dataset",
        "tdr_object_env": "dev"
    },
    "target": {
        "tdr_billing_profile": "72c87190-e50f-4fa5-80bd-44cd8780394f",
        "tdr_dataset_uuid": "",
        "tdr_dataset_name": "TDR_Migration_Tool_Test_1_20230915",
        "tdr_dataset_properties": {},
        "copy_policies": True
    },
    "ingest": {
        "records_fetching_method": "tdr_api",
        "records_processing_method": "in_memory", 
        "write_to_cloud_platform": "",
        "write_to_cloud_location": "",
        "write_to_cloud_sas_token": "",
        "max_records_per_ingest_request": 250000,
        "max_filerefs_per_ingest_request": 50000,
        "files_already_ingested": False,
        "tables_to_ingest": ["file_inventory", "subject", "anvil_donor"],
        "datarepo_row_ids_to_ingest": [],
        "apply_anvil_transforms": True
    },
    "snapshot": {
        "recreate_snapshot": True,
        "new_snapshot_name": "TDR_Migration_Tool_Test_1_20230915_SS",
        "copy_snapshot_policies": True
    }
}

"""

# Imports
import data_repo_client
import google.auth
import datetime
import os
import re
import sys
import logging
import argparse
from time import sleep
from google.cloud import bigquery
from google.cloud import storage
from azure.storage.blob import BlobClient
import pandas as pd
import json
import numpy as np
import math
import pprint

# Function to create argument parser
def create_arg_parser():
    # Define arguments to be collected by the script
    parser = argparse.ArgumentParser(description="Migrate TDR object to new TDR dataset.")
    parser.add_argument("-c", "--config_path", required=True, type=str, help="Path to the JSON configuration file for the job.")
    return parser

# Function to refresh TDR API client
def refresh_tdr_api_client(host):
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    config = data_repo_client.Configuration()
    config.host = host
    config.access_token = creds.token
    api_client = data_repo_client.ApiClient(configuration=config)
    api_client.client_side_validation = False
    return api_client

# Function to wait for TDR job completion
def wait_for_tdr_job(job_model, host):
    result = job_model
    print("TDR Job ID: " + job_model.id)
    counter = 0
    job_state = "UNKNOWN"
    while True:
        # Re-establish credentials and API clients every 30 minutes
        if counter == 0 or counter%180 == 0:
            api_client = refresh_tdr_api_client(host)
            jobs_api = data_repo_client.JobsApi(api_client=api_client)
        # Check for TDR connectivity issues and raise exception if the issue persists
        conn_err_counter = 0
        while job_state == "UNKNOWN":
            conn_err_counter += 1
            if conn_err_counter >= 10:
                raise Exception("Error interacting with TDR: {}".format(result.status_code)) 
            elif result == None or result.status_code in ["500", "502", "503", "504"]:
                sleep(10)
                counter += 1
                attempt_counter = 0
                while True:
                    try:
                        result = jobs_api.retrieve_job(job_model.id)
                        break
                    except Exception as e:
                        if attempt_counter < 5:
                            attempt_counter += 1
                            sleep(10)
                            continue
                        else:
                            raise Exception("Error retrieving job status from TDR: {}".format(str(e)))
            else:
                job_state = "KNOWN"
        # Check if job is still running, and sleep/re-check if so
        if job_state == "KNOWN" and result.job_status == "running":
            sleep(10)
            counter += 1
            attempt_counter = 0
            while True:
                try:
                    result = jobs_api.retrieve_job(job_model.id)
                    break
                except Exception as e:
                    if attempt_counter < 5:
                        sleep(10)
                        attempt_counter += 1
                        continue
                    else:
                        raise Exception("Error retrieving job status from TDR: {}".format(str(e)))
        # If job has returned as failed, confirm this is the correct state and retrieve result if so
        elif job_state == "KNOWN" and result.job_status == "failed":
            fail_counter = 0
            while True:
                attempt_counter = 0
                while True:
                    try:
                        result = jobs_api.retrieve_job(job_model.id)
                        if result.job_status == "failed":
                            fail_counter += 1
                        break
                    except Exception as e:
                        if attempt_counter < 5:
                            sleep(10)
                            attempt_counter += 1
                            continue
                        else:
                            raise Exception("Error retrieving job status from TDR: {}".format(str(e)))
                if fail_counter >= 3:
                    try:
                        fail_result = jobs_api.retrieve_job_result(job_model.id)
                        raise Exception("Job " + job_model.id + " failed: " + fail_result)
                    except Exception as e:
                        raise Exception("Job " + job_model.id + " failed: " + str(e))
        # If a job has returned as succeeded, retrieve result
        elif job_state == "KNOWN" and result.job_status == "succeeded":
            attempt_counter = 0
            while True:
                try:
                    return jobs_api.retrieve_job_result(job_model.id), job_model.id
                except Exception as e:
                    if attempt_counter < 3:
                        sleep(10)
                        attempt_counter += 1
                        continue
                    else:
                        return "Job succeeded, but error retrieving job result: {}".format(str(e)), job_model.id
        else:
            raise Exception("Unrecognized job state: {}".format(result.job_status))
                    
# Function to recreate snapshot, if requested
def recreate_snapshot(config, new_dataset_id):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    src_tdr_object_env = config["source"]["tdr_object_env"]
    tdr_host = config["source"]["tdr_host"]
    tar_tdr_billing_profile = config["target"]["tdr_billing_profile"]
    recreate_snapshot = config["snapshot"]["recreate_snapshot"]
    new_snapshot_name = config["snapshot"]["new_snapshot_name"]
    copy_snapshot_policies = config["snapshot"]["copy_snapshot_policies"]
    
    if recreate_snapshot:
        
        # Setup/refresh TDR clients
        api_client = refresh_tdr_api_client(tdr_host)
        datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
        snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
        
        # Retrieve original dataset details
        try:
            snapshot_details = snapshots_api.retrieve_snapshot(id=src_tdr_object_uuid, include=["TABLES", "RELATIONSHIPS", "ACCESS_INFORMATION", "PROPERTIES", "DATA_PROJECT", "SOURCES"]).to_dict()
            original_snapshot_name = snapshot_details["name"]
        except:
            err_str = f"Error retrieving details from original {src_tdr_object_type} {src_tdr_object_uuid} in TDR {src_tdr_object_env} environment."
            logging.error(err_str)
            config["migration_results"].append(["Snapshot Creation", "Snapshot Creation", "Failure", error_str])
            return
        
        # Retrieve new dataset name
        try:
            dataset_details = datasets_api.retrieve_dataset(id=new_dataset_id, include=["SCHEMA", "ACCESS_INFORMATION", "PROPERTIES", "DATA_PROJECT", "STORAGE"]).to_dict()
            new_dataset_name = dataset_details["name"]
            new_description = f"Copy of {src_tdr_object_type} {original_snapshot_name} from TDR {src_tdr_object_env}. Original description below:\n\n" + snapshot_details["description"]
        except:
            err_str = f"Error retrieving details from new dataset {new_dataset_id} in TDR {src_tdr_object_env} environment."
            logging.error(err_str)
            config["migration_results"].append(["Snapshot Creation", "Snapshot Creation", "Failure", error_str])
            return
            
        # Set desired snapshot policies
        snapshot_stewards_list = []
        snapshot_readers_list = []
        if copy_snapshot_policies:
            try:
                snapshot_policies = snapshots_api.retrieve_snapshot_policies(id=src_tdr_object_uuid).to_dict()
                for policy in snapshot_policies["policies"]:
                    if policy["name"] == "steward":
                        snapshot_stewards_list = policy["members"]
                    elif policy["name"] == "reader":
                        snapshot_readers_list = policy["members"]
            except:
                logging.warning("Error retrieving policies from original snapshot. Check permissions and add manually as needed.")
                
        # Create and submit snapshot creation request for Azure TDR dataset
        logging.info("Submitting snapshot request.")
        snapshot_req = {
            "name": new_snapshot_name,
            "description": new_description,
            "consentCode": snapshot_details["consent_code"],
            "contents": [{
                "datasetName": new_dataset_name,
                "mode": "byFullView"
            }],
            "policies": {
                "stewards": snapshot_stewards_list,
                "readers": snapshot_readers_list 
            },
            "profileId": tar_tdr_billing_profile,
            "globalFileIds": snapshot_details["global_file_ids"],
            "compactIdPrefix": snapshot_details["compact_id_prefix"],
            "properties": snapshot_details["properties"],
            "tags": snapshot_details["tags"]
        }
        attempt_counter = 1
        while True:
            try:
                create_snapshot_result, job_id = wait_for_tdr_job(snapshots_api.create_snapshot(snapshot=snapshot_req), tdr_host)
                logging.info("Snapshot Creation succeeded: {}".format(create_snapshot_result))
                config["migration_results"].append(["Snapshot Creation", "Snapshot Creation", "Success", str(create_snapshot_result)[0:1000]])
                break
            except Exception as e:
                logging.error("Error on Snapshot Creation: {}".format(str(e)))
                if attempt_counter < 3:
                    logging.info("Retrying Snapshot Creation (attempt #{})...".format(str(attempt_counter)))
                    sleep(10)
                    attempt_counter += 1
                    continue
                else:
                    logging.error("Maximum number of retries exceeded. Recording error to pipeline results.")
                    err_str = f"Error on Snapshot Creation: {str(e)}"
                    config["migration_results"].append(["Snapshot Creation", "Snapshot Creation", "Failure", err_str])
                    break
    else:
        config["migration_results"].append(["Snapshot Creation", "Snapshot Creation", "Skipped", ""])

# Function to write records to specified GCP location
def write_records_to_gcp(config, table, records_processed):
    # Extract parameters from config
    write_to_cloud_location = config["ingest"]["write_to_cloud_location"]

    # Write records to a file
    records_cnt = len(records_processed)
    destination_file = table + ".json"
    with open(destination_file, "w") as outfile:
        for idx, val in enumerate(records_processed):
            json.dump(val, outfile)
            if idx < records_cnt:
                outfile.write("\n")

    # Copy file to cloud
    if write_to_cloud_location[-1] == "/":
        target_cloud_path = write_to_cloud_location + destination_file
    else:
        target_cloud_path = write_to_cloud_location + "/" + destination_file
    client = storage.Client()
    target_bucket = target_cloud_path.split("/")[2]
    target_object = "/".join(target_cloud_path.split("/")[3:])
    bucket = client.bucket(target_bucket)
    blob = bucket.blob(target_object)
    blob.upload_from_filename(destination_file)
    
    # Remove local file
    if os.path.exists(destination_file):
        os.remove(destination_file)
    return target_cloud_path 

# Function to write records to specified GCP location
def write_records_to_azure(config, table, records_processed):
    # Extract parameters from config
    write_to_cloud_location = config["ingest"]["write_to_cloud_location"]
    write_to_cloud_sas_token = config["ingest"]["write_to_cloud_sas_token"]

    # Write records to a file
    records_cnt = len(records_processed)
    destination_file = table + ".json"
    with open(destination_file, "w") as outfile:
        for idx, val in enumerate(records_processed):
            json.dump(val, outfile)
            if idx < records_cnt:
                outfile.write("\n")

    # Copy file to cloud
    if write_to_cloud_location[-1] == "/":
        target_cloud_path = write_to_cloud_location + destination_file + "?" + write_to_cloud_sas_token
    else:
        target_cloud_path = write_to_cloud_location + "/" + destination_file + "?" + write_to_cloud_sas_token
    blob = BlobClient.from_blob_url(target_cloud_path)
    with open(destination_file, mode="rb") as data:
        blob.upload_blob(data=data, overwrite=True)
    
    # Remove local file
    if os.path.exists(destination_file):
        os.remove(destination_file)
    return target_cloud_path
                
# Function to fetch data from BigQuery
def fetch_source_records_bigquery(config, new_dataset_id, array_col_dict, table, start_row, end_row):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    tdr_host = config["source"]["tdr_host"]
    files_already_ingested = config["ingest"]["files_already_ingested"]
    datarepo_row_ids_to_ingest = config["ingest"]["datarepo_row_ids_to_ingest"]
    apply_anvil_transforms = config["ingest"]["apply_anvil_transforms"] 
    bq_project = config["source"]["bigquery_project"]
    bq_dataset = config["source"]["bigquery_dataset"]
    
    # Setup/refresh TDR clients (and BQ client)
    api_client = refresh_tdr_api_client(tdr_host)
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
    snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
    client = bigquery.Client(project=bq_project)
    
    # Retrieve table data from the original dataset
    logging.info(f"Fetching rows {str(start_row)}-{str(end_row)} from table '{table}' in the original {src_tdr_object_type} ({src_tdr_object_uuid}).")
    table_recs_str = f"Table: {table} -- Rows: {str(start_row)}-{str(end_row)}"
    final_records = []
    if apply_anvil_transforms and "anvil_" not in table:
        if table == "file_inventory":
            if files_already_ingested == False:
                file_ref_sql = "TO_JSON_STRING(STRUCT(source_name AS sourcePath, target_path AS targetPath, 'Ingest of '||source_name AS description, COALESCE(content_type, 'application/octet-stream') AS mimeType))"
            else:
                file_ref_sql = "file_ref"
            rec_fetch_query = f"""WITH drlh_deduped AS
                            (
                              SELECT DISTINCT file_id, target_path, source_name
                              FROM `{bq_project}.{bq_dataset}.datarepo_load_history`
                              WHERE state = "succeeded" 
                            )
                            SELECT * EXCEPT(rownum)
                            FROM
                            (
                              SELECT datarepo_row_id, datarepo_row_id AS orig_datarepo_row_id, a.file_id, name, path, uri, content_type, full_extension, size_in_bytes, crc32c, md5_hash, ingest_provenance,
                              file_ref AS orig_file_ref, {file_ref_sql} AS file_ref,
                              ROW_NUMBER() OVER (ORDER BY datarepo_row_id) AS rownum
                              FROM `{bq_project}.{bq_dataset}.{table}` a
                                  LEFT JOIN drlh_deduped b
                                  ON a.file_ref = b.file_id
                            )
                            WHERE rownum BETWEEN {start_row} AND {end_row}"""
        else:
            rec_fetch_query = f"""SELECT * EXCEPT(rownum)
                            FROM
                            (
                              SELECT *, datarepo_row_id AS orig_datarepo_row_id,
                              ROW_NUMBER() OVER (ORDER BY datarepo_row_id) AS rownum
                              FROM `{bq_project}.{bq_dataset}.{table}`
                            )
                            WHERE rownum BETWEEN {start_row} AND {end_row}"""
    else:
        rec_fetch_query = f"""SELECT * EXCEPT(rownum)
                            FROM
                            (
                              SELECT *, 
                              ROW_NUMBER() OVER (ORDER BY datarepo_row_id) AS rownum
                              FROM `{bq_project}.{bq_dataset}.{table}`
                            )
                            WHERE rownum BETWEEN {start_row} AND {end_row}"""
    attempt_counter = 0
    while True:
        try:
            df = client.query(rec_fetch_query).result().to_dataframe()
            df = df.astype(object).where(pd.notnull(df),None)
            for column in array_col_dict[table]:
                df[column] = df[column].apply(lambda x: list(x))
            if apply_anvil_transforms and table == "file_inventory" and files_already_ingested == False: 
                df["file_ref"] = df.apply(lambda x: json.loads(x["file_ref"].replace("\'", "\"")), axis=1)
            final_records = df.to_dict(orient="records")
            break
        except Exception as e:
            if attempt_counter < 5:
                sleep(10)
                attempt_counter += 1
                continue
            else:
                err_str = f"Error retrieving records for rows {str(start_row)}-{str(end_row)} of table {table}: {str(e)}."
                logging.error(err_str)
                config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Failure", err_str])
                return {}
    
    # Filter retrieved data if necessary and return as dict of records
    if final_records:
        df_temp = pd.DataFrame.from_dict(final_records)
        if datarepo_row_ids_to_ingest:
            df_orig = df_temp[df_temp["datarepo_row_id"].isin(datarepo_row_ids_to_ingest)].copy()
        else:
            df_orig = df_temp.copy()
        del df_temp
        df_orig.drop(columns=["datarepo_row_id"], inplace=True, errors="ignore")
        df_orig = df_orig.astype(object).where(pd.notnull(df_orig),None)
        records_orig = df_orig.to_dict(orient="records")
        if not records_orig:
            msg_str = f"No records found in rows {str(start_row)}-{str(end_row)} of table {table} after filtering based on datarepo_row_ids_to_ingest parameter. Continuing to next record set or table validation."
            logging.info(msg_str)
            config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Skipped", msg_str])
            return records_orig
        elif len(final_records) != len(records_orig):
            logging.info(f"Filtering records to ingest based on the datarepo_row_ids_to_ingest parameter. {str(len(records_orig))} of {str(len(final_records))} records to be ingested.")
            return records_orig
        else:
            return records_orig
    else:
        msg_str = f"No records found for rows {str(start_row)}-{str(end_row)} of table {table} in original {src_tdr_object_type}. Continuing to next record set or table validation."
        logging.info(msg_str)
        config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Skipped", msg_str])
        return final_records
        
# Function to fetch data from TDR API
def fetch_source_records_tdr_api(config, new_dataset_id, table, start_row, end_row):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    tdr_host = config["source"]["tdr_host"]
    datarepo_row_ids_to_ingest = config["ingest"]["datarepo_row_ids_to_ingest"]
    
    # Setup/refresh TDR clients
    api_client = refresh_tdr_api_client(tdr_host)
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
    snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
    
    # Retrieve table data from the original dataset
    logging.info(f"Fetching rows {str(start_row)}-{str(end_row)} from table '{table}' in the original {src_tdr_object_type} ({src_tdr_object_uuid}).")
    table_recs_str = f"Table: {table} -- Rows: {str(start_row)}-{str(end_row)}"
    max_page_size = 1000
    total_records_fetched = start_row - 1
    final_records = []
    while True:
        offset = total_records_fetched
        page_size = min(max_page_size, end_row - total_records_fetched)
        attempt_counter = 0
        while True:
            payload = {
              "offset": offset,
              "limit": page_size,
              "sort": "datarepo_row_id",
              "direction": "asc",
              "filter": ""
            }
            try:
                if src_tdr_object_type == "dataset":
                    record_results = datasets_api.query_dataset_data_by_id(id=src_tdr_object_uuid, table=table, query_data_request_model=payload).to_dict() 
                elif src_tdr_object_type == "snapshot":
                    record_results = snapshots_api.query_snapshot_data_by_id(id=src_tdr_object_uuid, table=table, query_data_request_model=payload).to_dict() 
                else:
                    raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.")
                break
            except Exception as e:
                if attempt_counter < 5:
                    sleep(10)
                    attempt_counter += 1
                    continue
                else:
                    err_str = f"Error retrieving records for rows {str(start_row)}-{str(end_row)} of table {table}: {str(e)}."
                    logging.error(err_str)
                    config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Failure", err_str])
                    return {}
        if record_results["result"]:
            final_records.extend(record_results["result"])
            total_records_fetched += len(record_results["result"])
        else:
            break
        if total_records_fetched >= end_row:
            break
    
    # Filter retrieved data if necessary and return as dict of records
    if final_records:
        df_temp = pd.DataFrame.from_dict(final_records)
        if datarepo_row_ids_to_ingest:
            df_orig = df_temp[df_temp["datarepo_row_id"].isin(datarepo_row_ids_to_ingest)].copy()
        else:
            df_orig = df_temp.copy()
        del df_temp
        df_orig.drop(columns=["datarepo_row_id"], inplace=True, errors="ignore")
        records_orig = df_orig.to_dict(orient="records")
        if not records_orig:
            msg_str = f"No records found in rows {str(start_row)}-{str(end_row)} of table {table} after filtering based on datarepo_row_ids_to_ingest parameter. Continuing to next record set or table validation."
            logging.info(msg_str)
            config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Skipped", msg_str])
            return records_orig
        elif len(final_records) != len(records_orig):
            logging.info(f"Filtering records to ingest based on the datarepo_row_ids_to_ingest parameter. {str(len(records_orig))} of {str(len(final_records))} records to be ingested.")
            return records_orig
        else:
            return records_orig
    else:
        msg_str = f"No records found for rows {str(start_row)}-{str(end_row)} of table {table} in original {src_tdr_object_type}. Continuing to next record set or table validation."
        logging.info(msg_str)
        config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Skipped", msg_str])
        return records_orig

# Function to process ingests for specific table
def ingest_table_data(config, new_dataset_id, fileref_col_dict, array_col_dict, table, start_row, end_row, source_file_list):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    src_tdr_object_cloud = config["source"]["tdr_object_cloud"]
    tdr_host = config["source"]["tdr_host"]
    tar_tdr_billing_profile = config["target"]["tdr_billing_profile"]
    records_fetching_method = config["ingest"]["records_fetching_method"]
    records_processing_method = config["ingest"]["records_processing_method"]
    write_to_cloud_platform = config["ingest"]["write_to_cloud_platform"]
    apply_anvil_transforms = config["ingest"]["apply_anvil_transforms"] 

    # Setup/refresh TDR clients
    api_client = refresh_tdr_api_client(tdr_host)
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
    snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
    
    # Retrieve table data from the original dataset
    table_recs_str = f"Table: {table} -- Rows: {str(start_row)}-{str(end_row)}"
    if records_fetching_method == "cloud_native" and src_tdr_object_type == "azure":
        logging.info("Record fetching method 'cloud_native' not yet supported for Azure source TDR objects. Using the 'tdr_api' method instead.")
        records_orig = fetch_source_records_tdr_api(config, new_dataset_id, table, start_row, end_row) 
    elif records_fetching_method == "tdr_api":
        records_orig = fetch_source_records_tdr_api(config, new_dataset_id, table, start_row, end_row)        
    else:
        records_orig = fetch_source_records_bigquery(config, new_dataset_id, array_col_dict, table, start_row, end_row) 
    if not records_orig:
        return

    # Pre-process records before ingest
    if fileref_col_dict[table] and not apply_anvil_transforms:
        try:
            # Pre-process records to include file reference objects
            logging.info("File reference columns present. Pre-processing records before submitting ingestion request.")
            api_client = refresh_tdr_api_client(tdr_host)
            datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
            snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
            records_processed = []
            for record in records_orig:
                int_record = record.copy()
                for fileref_col in fileref_col_dict[table]:
                    if isinstance(int_record[fileref_col], list):
                        fileref_obj_list = []
                        for val in int_record[fileref_col]:
                            if val:
                                file_id = val if src_tdr_object_type == "dataset" else re.match(r".*_([^_]+$)", val).group(1)
                                file_results = source_file_list.get(file_id)
                                if file_results:
                                    fileref_obj = {
                                        "sourcePath": file_results["source_url"],
                                        "targetPath": file_results["file_path"],
                                        "description": file_results["description"],
                                        "mimeType": file_results["mime_type"]
                                    }
                                    fileref_obj_list.append(fileref_obj)
                        int_record[fileref_col] = fileref_obj_list    
                    elif int_record[fileref_col]:
                        fileref_obj = {}
                        file_id = int_record[fileref_col] if src_tdr_object_type == "dataset" else re.match(r".*_([^_]+$)", int_record[fileref_col]).group(1)
                        file_results = source_file_list.get(file_id)
                        if file_results:
                            fileref_obj = {
                                "sourcePath": file_results["source_url"],
                                "targetPath": file_results["file_path"],
                                "description": file_results["description"],
                                "mimeType": file_results["mime_type"]
                            }
                            int_record[fileref_col] = fileref_obj
                records_processed.append(int_record)
        except Exception as e:
            err_str = f"Failure in pre-processing: {str(e)}"
            config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Failure", err_str])
            return
    elif apply_anvil_transforms and "anvil_" in table:
        try:
            # Pre-process records in AnVIL_ records to use new datarepo_row_ids in the source_datarepo_row_ids field
            logging.info("FSS (anvil_%) table with ingest.apply_anvil_transforms parameter set to 'True'. Pre-processing records before submitting ingestion request.")
            records_processed = []
            for record in records_orig:
                int_record = record.copy()
                new_dr_row_id_list = []
                for row_id in int_record["source_datarepo_row_ids"]:
                    new_row_id = config["anvil"]["dr_row_id_xwalk"].get(row_id)
                    if new_row_id:
                        new_dr_row_id_list.append(new_row_id)
                int_record["source_datarepo_row_ids"] = new_dr_row_id_list
                records_processed.append(int_record)
        except Exception as e:
            err_str = f"Failure in pre-processing: {str(e)}"
            config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Failure", err_str])
            return
    else:
        records_processed = records_orig    
    
    # Write out records to cloud, if specified by user
    if records_processing_method == "write_to_cloud":
        logging.info(f"Writing records to a control file in the cloud.")
        if write_to_cloud_platform == "gcp":
            control_file_path = write_records_to_gcp(config, table, records_processed)
        else:
            control_file_path = write_records_to_azure(config, table, records_processed)

    # Build, submit, and monitor ingest request
    logging.info(f"Submitting ingestion request to new dataset ({new_dataset_id}).")
    if records_processing_method == "write_to_cloud":
        ingest_request = {
            "table": table,
            "profile_id": tar_tdr_billing_profile,
            "ignore_unknown_values": True,
            "resolve_existing_files": True,
            "updateStrategy": "append",
            "format": "json",
            "load_tag": "Ingest for {}".format(new_dataset_id),
            "path": control_file_path
        }        
    else:
        ingest_request = {
            "table": table,
            "profile_id": tar_tdr_billing_profile,
            "ignore_unknown_values": True,
            "resolve_existing_files": True,
            "updateStrategy": "append",
            "format": "array",
            "load_tag": "Ingest for {}".format(new_dataset_id),
            "records": records_processed
        }
    attempt_counter = 1
    while True:
        try:
            api_client = refresh_tdr_api_client(tdr_host)
            datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
            ingest_request_result, job_id = wait_for_tdr_job(datasets_api.ingest_dataset(id=new_dataset_id, ingest=ingest_request), tdr_host)
            logging.info("Ingest succeeded: {}".format(str(ingest_request_result)[0:1000]))
            config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Success", str(ingest_request_result)[0:1000]])
            break
        except Exception as e:
            logging.error("Error on ingest: {}".format(str(e)[0:2500]))
            if attempt_counter < 3:
                logging.info("Retrying ingest (attempt #{})...".format(str(attempt_counter)))
                sleep(10)
                attempt_counter += 1
                continue
            else:
                logging.error("Maximum number of retries exceeded. Logging error.")
                err_str = f"Error on ingest: {str(e)[0:2500]}"
                config["migration_results"].append(["Dataset Ingestion", table_recs_str, "Failure", err_str])  
                break

    # Remove control file from cloud, if written out
    if records_processing_method == "write_to_cloud":
        logging.info(f"Removing control file from the cloud.")
        if write_to_cloud_platform == "gcp":
            client = storage.Client()
            target_bucket = control_file_path.split("/")[2]
            target_object = "/".join(control_file_path.split("/")[3:])
            bucket = client.bucket(target_bucket)
            blob = bucket.blob(target_object)
            blob.delete()
        else:
            blob = BlobClient.from_blob_url(control_file_path)
            blob.delete_blob()

# Function to build a list of files in the source TDR object for use in populating the new TDR dataset
def build_source_file_list(config):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    tdr_host = config["source"]["tdr_host"]

    # Setup/refresh TDR clients
    api_client = refresh_tdr_api_client(tdr_host)
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
    snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)

    # Retrieve file list from TDR object
    source_file_dict = {}
    logging.info(f"Retrieving files from source {src_tdr_object_type}.")
    max_page_size = 1000
    total_records_fetched = 0
    while True:
        row_start = total_records_fetched
        if src_tdr_object_type == "snapshot":
            file_results = snapshots_api.list_files(id=src_tdr_object_uuid, offset=row_start, limit=max_page_size)
        else:
            file_results = datasets_api.list_files(id=src_tdr_object_uuid, offset=row_start, limit=max_page_size)
        if file_results:
            total_records_fetched += len(file_results)
            for entry in file_results:
                record = entry.to_dict()
                file_detail_dict = {
                    "file_path": record["path"],
                    "file_name": os.path.basename(record["path"]),
                    "source_url": record["file_detail"]["access_url"],
                    "file_size": record["size"],
                    "description": record["description"],
                    "mime_type": record["file_detail"]["mime_type"]
                }
                file_id = record["file_id"]
                source_file_dict[file_id] = file_detail_dict
            logging.info(f"{total_records_fetched} records fetched...")
        else:
            break
    file_len = len(source_file_dict)
    logging.info(f"File retrieval complete. {file_len} files found.")
    return source_file_dict

# Function to populate new TDR dataset
def populate_new_dataset(config, new_dataset_id, fileref_col_dict, array_col_dict):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    src_tdr_object_env = config["source"]["tdr_object_env"]
    tdr_host = config["source"]["tdr_host"]
    tar_tdr_billing_profile = config["target"]["tdr_billing_profile"]
    tdr_general_sa = config["tdr_general_sa"]
    chunk_size = config["ingest"]["max_records_per_ingest_request"]
    max_combined_rec_ref_size = config["ingest"]["max_filerefs_per_ingest_request"]
    tables_to_ingest = config["ingest"]["tables_to_ingest"]
    datarepo_row_ids_to_ingest = config["ingest"]["datarepo_row_ids_to_ingest"]
    apply_anvil_transforms = config["ingest"]["apply_anvil_transforms"] 

    # Setup/refresh TDR clients
    api_client = refresh_tdr_api_client(tdr_host)
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
    snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
    
    # Retrieve TDR SA to add from new dataset
    logging.info(f"Adding TDR SA to original {src_tdr_object_type}: {src_tdr_object_uuid}")
    try:
        dataset_details = datasets_api.retrieve_dataset(id=new_dataset_id).to_dict()
        if dataset_details["ingest_service_account"]:
            tdr_sa_to_use = dataset_details["ingest_service_account"]
        else:
            tdr_sa_to_use = tdr_general_sa
    except:
        error_str = f"Error retrieving details from dataset {new_dataset_id} in TDR {src_tdr_object_env} environment."
        logging.error(error_str)
        config["migration_results"].append(["Dataset Ingestion", "All Tables", "Failure", error_str])
        return
    logging.info(f"TDR SA to add: {tdr_sa_to_use}")

    # Add TDR SA to original object, if not already present
    sa_user_already_present = False
    try:
        if src_tdr_object_type == "dataset":
            resp = datasets_api.retrieve_dataset_policies(id=src_tdr_object_uuid).to_dict()
        elif src_tdr_object_type == "snapshot":
            resp = snapshots_api.retrieve_snapshot_policies(id=src_tdr_object_uuid).to_dict()
        else:
            raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.")
        for policy in resp["policies"]:
            if policy["name"] in ["custodian", "steward", "admin", "snapshot_creator", "reader"] and tdr_sa_to_use in policy["members"]:
                sa_user_already_present = True
                break
    except Exception as e:
        error_str = f"Error retrieving existing policies from {src_tdr_object_type} {src_tdr_object_uuid} in TDR {src_tdr_object_env} environment: {str(e)}"
        logging.error(error_str)
        config["migration_results"].append(["Dataset Ingestion", "All Tables", "Failure", error_str])
        return
    if not sa_user_already_present:
        try:
            if src_tdr_object_type == "dataset":
                resp = datasets_api.add_dataset_policy_member(id=src_tdr_object_uuid, policy_name="snapshot_creator", policy_member={"email": tdr_sa_to_use}) 
            elif src_tdr_object_type == "snapshot":
                resp = snapshots_api.add_snapshot_policy_member(id=src_tdr_object_uuid, policy_name="reader", policy_member={"email": tdr_sa_to_use}) 
            else:
                raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.")
            logging.info("TDR SA added successfully. Pausing processing for a few minutes to allow for permissions to propagate.")
            sleep(600)
        except Exception as e:
            error_str = f"Error adding TDR SA to {src_tdr_object_type} {src_tdr_object_uuid} in TDR {src_tdr_object_env} environment: {str(e)}"
            logging.error(error_str)
            config["migration_results"].append(["Dataset Ingestion", "All Tables", "Failure", error_str])
            return
    else:
        logging.info("TDR SA already present on original {src_tdr_object_type}: {src_tdr_object_uuid}. Continuing processing.")
    
    # Pull a list of files from the source TDR object for use in table processing
    source_file_list = build_source_file_list(config)

    # Loop through and process tables for ingestion
    logging.info("Processing dataset ingestion requests.")
    if apply_anvil_transforms:
        config["anvil"] = {}
        config["anvil"]["dr_row_id_xwalk"] = {}
        table_rank_dict = {}
        for table in fileref_col_dict.keys():
            if table == "file_inventory":
                table_rank_dict[table] = 1
            elif "anvil_" not in table:
                table_rank_dict[table] = 2
            else:
                table_rank_dict[table] = 3
        ordered_table_list = sorted(table_rank_dict, key= lambda key: table_rank_dict[key])
    else:
        ordered_table_list = sorted(fileref_col_dict, key=lambda key: (len(fileref_col_dict[key]), key))
    for table in ordered_table_list:
        
        # Determine whether table should be processed, and skip if not
        logging.info(f"Processing dataset ingestion for table '{table}'.")
        if tables_to_ingest and table not in tables_to_ingest:
            msg_str = f"Table '{table}' not listed in the ingest.tables_to_ingest parameter. Skipping."
            logging.info(msg_str)
            config["migration_results"].append(["Dataset Ingestion", f"Table: {table}", "Skipped", msg_str])
            continue
        
        # Fetch total record count for table
        api_client = refresh_tdr_api_client(tdr_host)
        datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
        snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
        attempt_counter = 0
        while True:
            payload = {
              "offset": 0,
              "limit": 10,
              "sort": "datarepo_row_id",
              "direction": "asc",
              "filter": ""
            }
            try:
                if src_tdr_object_type == "dataset":
                    record_results = datasets_api.query_dataset_data_by_id(id=src_tdr_object_uuid, table=table, query_data_request_model=payload).to_dict()
                elif src_tdr_object_type == "snapshot":
                    record_results = snapshots_api.query_snapshot_data_by_id(id=src_tdr_object_uuid, table=table, query_data_request_model=payload).to_dict() 
                else:
                    raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.")
                total_record_count = record_results["total_row_count"]
                break
            except Exception as e:
                logging.error(str(e))
                if attempt_counter < 5:
                    sleep(10)
                    attempt_counter += 1
                    continue
                else:
                    total_record_count = -1
                    break
        if total_record_count == -1:
            err_str = f"Error retrieving record count for table '{table}' in original {src_tdr_object_type}. Continuing to next table."
            logging.error(err_str)
            config["migration_results"].append(["Dataset Ingestion", f"Table: {table}", "Failure", err_str])
            continue 
        elif total_record_count == 0:
            msg_str = f"No records found for table in original {src_tdr_object_type}. Continuing to next table/record set."
            logging.info(msg_str)
            config["migration_results"].append(["Dataset Ingestion", f"Table: {table}", "Skipped", msg_str])
            continue
        
        # Chunk table records as necessary, then loop through and process each chunk
        if fileref_col_dict[table]:
            ref_chunk_size = math.floor(max_combined_rec_ref_size / len(fileref_col_dict[table]))
            chunk_size = min(chunk_size, ref_chunk_size)
            logging.info(f"Table '{table}' contains fileref columns. Will use a chunk size of {chunk_size} rows per ingestion request, to keep the number of file references per chunk below {max_combined_rec_ref_size}.")
        else:
            logging.info(f"Table '{table}' does not contain fileref columns. Will use a chunk size of {chunk_size} rows per ingestion request.")
        start_row = 1
        end_row = min((chunk_size), total_record_count)
        while start_row <= total_record_count:
            if end_row > total_record_count:
                end_row = total_record_count
            ingest_table_data(config, new_dataset_id, fileref_col_dict, array_col_dict, table, start_row, end_row, source_file_list)    
            start_row += chunk_size
            end_row += chunk_size
            
        # Fetch total record count for the new table
        api_client = refresh_tdr_api_client(tdr_host)
        datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
        snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
        while True:
            payload = {
              "offset": 0,
              "limit": 10,
              "sort": "datarepo_row_id",
              "direction": "asc",
              "filter": ""
            }
            try:
                record_results = datasets_api.query_dataset_data_by_id(id=new_dataset_id, table=table, query_data_request_model=payload).to_dict()
                new_record_count = record_results["total_row_count"]
                break
            except Exception as e:
                if attempt_counter < 5:
                    sleep(10)
                    attempt_counter += 1
                    continue
                else:
                    new_record_count = -1
                    break
        if new_record_count == -1:
            err_str = f"Error retrieving record count for table '{table}' in new dataset. Skipping validation and continuing to next table."
            logging.error(err_str)
            config["migration_results"].append(["Dataset Validation", f"Table: {table}", "Failure", err_str])
            continue 
        
        # Validate the new table against the old table, with extra scrutiny given to the file_inventory table for AnVIL migrations
        logging.info(f"Validating table '{table}' in new dataset vs. original {src_tdr_object_type}.")
        if apply_anvil_transforms and table == "file_inventory":
            err_msg = f"Validation error with file_inventory table for job with ingest.apply_anvil_transforms parameter set to 'True'. Due to downstream dependencies on this table, skipping remaining tables and failing job."
            if new_record_count != total_record_count:
                config["migration_results"].append(["Dataset Validation", f"Table: {table}", "Failure", f"{new_record_count} records found in new table doesn't match {total_record_count} records in original table."])
                config["migration_results"].append(["Dataset Ingestion", f"Remaining Tables", "Skipped", err_msg])
                logging.error(err_msg)
                return
            else:
                api_client = refresh_tdr_api_client(tdr_host)
                datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
                max_page_size = 1000
                records_fetched = 0
                errors_found = []
                retrieval_error = False
                while records_fetched < total_record_count and not retrieval_error:
                    row_start = records_fetched
                    attempt_counter = 0
                    while True:
                        payload = {
                          "offset": row_start,
                          "limit": max_page_size,
                          "sort": "datarepo_row_id",
                          "direction": "asc",
                          "filter": ""
                        }
                        try:
                            dataset_results = datasets_api.query_dataset_data_by_id(id=new_dataset_id, table=table, query_data_request_model=payload).to_dict() 
                            for record in dataset_results["result"]:
                                key = table + ":" + record["orig_datarepo_row_id"]
                                val = table + ":" + record["datarepo_row_id"]
                                config["anvil"]["dr_row_id_xwalk"][key] = val
                                records_fetched += 1
                                if record["file_ref"] != record["orig_file_ref"] and len(errors_found) < 5:
                                    errors_found.append(record)
                            break
                        except Exception as e:
                            if attempt_counter < 5:
                                sleep(10)
                                attempt_counter += 1
                                continue
                            else:
                                warn_str = "Error retrieving records for 'file_inventory' table for job with ingest.apply_anvil_transforms parameter set to 'True'. Skipping comparison of file_ref and orig_file_ref fields. Note that mismatches between these fields may cause issues with ingest jobs downstream."
                                logging.warning(warn_str)
                                config["migration_results"].append(["Dataset Validation", f"Table: {table}", "Skipped", warn_str])
                                retrieval_error = True
                                break
                if errors_found:
                    config["migration_results"].append(["Dataset Validation", f"Table: {table}", "Failure", f"Records exist with mismatching file_ref and orig_file_ref_values. Sample records: {str(errors_found)}"])
                    config["migration_results"].append(["Dataset Ingestion", f"Remaining Tables", "Skipped", err_msg])
                    logging.error(err_msg)
                    return
                else:
                    config["migration_results"].append(["Dataset Validation", f"Table: {table}", "Success", f"{new_record_count} records found in both new and original table. No mismatches between file_ref and orig_file_ref found."])
        else:
            if new_record_count == total_record_count:
                config["migration_results"].append(["Dataset Validation", f"Table: {table}", "Success", f"{new_record_count} records found in both new and original table."])
            else:
                config["migration_results"].append(["Dataset Validation", f"Table: {table}", "Failure", f"{new_record_count} records found in new table doesn't match {total_record_count} records in original table."])
        
        # Build datarepo_row_id crosswalk for use in AnVIL migrations
        if apply_anvil_transforms and table != "file_inventory" and "anvil_" not in table: 
            logging.info("Fetching ingested records and building datarepo_row_id lookup for use in AnVIL transforms.")
            api_client = refresh_tdr_api_client(tdr_host)
            datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
            max_page_size = 1000
            records_fetched = 0
            retrieval_error = False
            while records_fetched < total_record_count and not retrieval_error:
                row_start = records_fetched
                attempt_counter = 0
                while True:
                    payload = {
                      "offset": row_start,
                      "limit": max_page_size,
                      "sort": "datarepo_row_id",
                      "direction": "asc",
                      "filter": ""
                    }
                    try:
                        dataset_results = datasets_api.query_dataset_data_by_id(id=new_dataset_id, table=table, query_data_request_model=payload).to_dict() 
                        for record in dataset_results["result"]:
                            key = table + ":" + record["orig_datarepo_row_id"]
                            val = table + ":" + record["datarepo_row_id"]
                            config["anvil"]["dr_row_id_xwalk"][key] = val
                            records_fetched += 1
                        break
                    except Exception as e:
                        if attempt_counter < 5:
                            sleep(10)
                            attempt_counter += 1
                            continue
                        else:
                            warn_str = f"Error retrieving records for '{table}' table for job with ingest.apply_anvil_transforms parameter set to 'True'. Note that this may cause issues with datarepo_row_id look-ups downstream."
                            logging.warning(warn_str)
                            retrieval_error = True
                            break
        
# Function to create a new TDR dataset from an existing TDR dataset
def create_dataset_from_dataset(config):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    src_tdr_object_env = config["source"]["tdr_object_env"]
    tdr_host = config["source"]["tdr_host"]
    tdr_general_sa = config["tdr_general_sa"]
    tar_tdr_billing_profile = config["target"]["tdr_billing_profile"]
    tar_tdr_dataset_uuid = config["target"]["tdr_dataset_uuid"]
    tar_tdr_dataset_name = config["target"]["tdr_dataset_name"]
    tar_tdr_dataset_cloud = config["target"]["tdr_dataset_cloud"]
    tar_tdr_dataset_props = config["target"]["tdr_dataset_properties"]
    copy_policies = config["target"]["copy_policies"] 
    apply_anvil_transforms = config["ingest"]["apply_anvil_transforms"] 

    # Setup/refresh TDR clients
    api_client = refresh_tdr_api_client(tdr_host)
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)

    # Retrieve original dataset details
    logging.info(f"Retrieving original {src_tdr_object_type} details from {src_tdr_object_env} environment. UUID:  {src_tdr_object_uuid}")
    try:
        dataset_details = datasets_api.retrieve_dataset(id=src_tdr_object_uuid, include=["SCHEMA", "ACCESS_INFORMATION", "PROPERTIES", "DATA_PROJECT", "STORAGE"]).to_dict()
    except Exception as e:
        error_str = f"Error retrieving details from {src_tdr_object_type} {src_tdr_object_uuid} in TDR {src_tdr_object_env} environment: {str(e)}"
        logging.error(error_str)
        config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Failure", error_str])
        return None, {}, {}
    
    # Validate source cloud platform
    #config["source"]["tdr_object_cloud"] = dataset_details["cloud_platform"] # This is null in the API endpoint at the moment
    config["source"]["tdr_object_cloud"] = dataset_details["storage"][0]["cloud_platform"]
    if config["source"]["tdr_object_cloud"] == "azure":
        config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Failure", "Migrate of Azure TDR objects is not yet supported. Try again with a GCP TDR object."])
        return None, {}, {}
    else:
        config["source"]["bigquery_project"] = dataset_details["access_information"]["big_query"]["project_id"]
        config["source"]["bigquery_dataset"] = dataset_details["access_information"]["big_query"]["dataset_name"]

    # Build new dataset schema
    new_schema_dict = {"tables": [], "relationships": [], "assets": []}
    fileref_col_dict = {}
    array_col_dict = {}
    for table_entry in dataset_details["schema"]["tables"]:
        int_table_dict = table_entry.copy()
        int_table_dict["primaryKey"] = int_table_dict.pop("primary_key")
        for key in ["partition_mode", "date_partition_options", "int_partition_options", "row_count"]:
            del int_table_dict[key]
        fileref_list = []
        array_list = []
        for idx, column_entry in enumerate(table_entry["columns"]):
            if column_entry["datatype"] == "fileref":
                fileref_list.append(column_entry["name"])
            if column_entry["array_of"] == True:
                array_list.append(column_entry["name"])
            if tar_tdr_dataset_cloud == "azure" and column_entry["datatype"] == "integer":
                table_entry["columns"][idx]["datatype"] = "int64"
        fileref_col_dict[table_entry["name"]] = fileref_list
        array_col_dict[table_entry["name"]] = array_list
        if apply_anvil_transforms:
            if table_entry["name"] == "file_inventory":
                int_table_dict["columns"].append({"name": "orig_file_ref", "datatype": "string", "array_of": False, "required": False})
                int_table_dict["columns"].append({"name": "orig_datarepo_row_id", "datatype": "string", "array_of": False, "required": False})
            elif "anvil_" not in table_entry["name"]:
                int_table_dict["columns"].append({"name": "orig_datarepo_row_id", "datatype": "string", "array_of": False, "required": False})
        new_schema_dict["tables"].append(int_table_dict)
    for rel_entry in dataset_details["schema"]["relationships"]:
        int_rel_dict = rel_entry.copy()
        int_rel_dict["from"] = int_rel_dict.pop("_from")
        new_schema_dict["relationships"].append(int_rel_dict)
    for asset_entry in dataset_details["schema"]["assets"]:
        int_asset_dict = asset_entry.copy()
        int_asset_dict["rootTable"] = int_asset_dict.pop("root_table")
        int_asset_dict["rootColumn"] = int_asset_dict.pop("root_column")
        new_schema_dict["assets"].append(int_asset_dict)

    # Create a new dataset, unless a target dataset UUID has been provided
    if tar_tdr_dataset_uuid:
        new_dataset_id = tar_tdr_dataset_uuid
        msg_str = f"Attempting to leverage user-provided target dataset UUID ({tar_tdr_dataset_uuid}) rather than creating a new dataset."
        logging.info(msg_str)
        config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Skipped", msg_str])
    else:
        # Retrieve original dataset policies
        if copy_policies:
            try:
                dataset_policies = datasets_api.retrieve_dataset_policies(id=src_tdr_object_uuid).to_dict()
                for policy in dataset_policies["policies"]:
                    if policy["name"] == "steward":
                        stewards_list = policy["members"]
                    elif policy["name"] == "custodian":
                        custodians_list = policy["members"]
                    elif policy["name"] == "snapshot_creator":
                        snapshot_creators_list = policy["members"]
            except:
                logging.info("Error retrieving original dataset policies. Skipping policy copy.")
                stewards_list = []
                custodians_list = []
                snapshot_creators_list = []
        else:
            stewards_list = []
            custodians_list = []
            snapshot_creators_list = []

        # Determine dataset properties - Description
        orig_object_name = dataset_details["name"]
        new_description = dataset_details["description"] + f"\n\nCopy of {src_tdr_object_type} {orig_object_name} from TDR {src_tdr_object_env}."
        description = tar_tdr_dataset_props["description"] if tar_tdr_dataset_props.get("description") else new_description

        # Determine dataset properties - Region
        for storage_entry in dataset_details["storage"]:
            if storage_entry["cloud_resource"] == "bucket":
                orig_region = storage_entry["region"]
                break
        if tar_tdr_dataset_props.get("region"):
            dataset_region = tar_tdr_dataset_props["region"]
        elif tar_tdr_dataset_cloud == "gcp" and orig_region:
            dataset_region = orig_region
        else:
            dataset_region = None

        # Determine dataset properties - Dedicated Ingest SA
        dedicated_ingest_sa = tar_tdr_dataset_props.get("dedicatedIngestServiceAccount")
        if dedicated_ingest_sa == None:
            dedicated_ingest_sa = False if dataset_details["ingest_service_account"] == tdr_general_sa else True

        # Determine dataset properties - Self-Hosted
        self_hosted = False
        if tar_tdr_dataset_cloud == "azure":
            self_hosted = False
        elif tar_tdr_dataset_props.get("experimentalSelfHosted"):
            self_hosted = tar_tdr_dataset_props["experimentalSelfHosted"]
        else:
            self_hosted = dataset_details["self_hosted"]

        # Determine dataset properties - Policies
        policies = {}
        if tar_tdr_dataset_props.get("policies"):
            if tar_tdr_dataset_props["policies"].get("stewards"):
                for user in tar_tdr_dataset_props["policies"]["stewards"]:
                    if user not in stewards_list:
                        stewards_list.append(user)
            if tar_tdr_dataset_props["policies"].get("custodians"):
                for user in tar_tdr_dataset_props["policies"]["custodians"]:
                    if user not in custodians_list:
                        custodians_list.append(user)
            if tar_tdr_dataset_props["policies"].get("snapshotCreators"):
                for user in tar_tdr_dataset_props["policies"]["snapshotCreators"]:
                    if user not in snapshot_creators_list:
                        snapshot_creators_list.append(user)
        policies = {
            "stewards": stewards_list,
            "custodians": custodians_list,
            "snapshotCreators": snapshot_creators_list
        }

        # Determine dataset properties - Other
        phs_id = tar_tdr_dataset_props["phsId"] if tar_tdr_dataset_props.get("phsId") else dataset_details["phs_id"]
        predictable_file_ids = tar_tdr_dataset_props["experimentalPredictableFileIds"] if tar_tdr_dataset_props.get("experimentalPredictableFileIds") else dataset_details["predictable_file_ids"]
        secure_monitoring_enabled = tar_tdr_dataset_props["enableSecureMonitoring"] if tar_tdr_dataset_props.get("enableSecureMonitoring") else dataset_details["secure_monitoring_enabled"]
        properties = tar_tdr_dataset_props["properties"] if tar_tdr_dataset_props.get("properties") else dataset_details["properties"]
        tags = tar_tdr_dataset_props["tags"] if tar_tdr_dataset_props.get("tags") else dataset_details["tags"]

        # Create new TDR dataset
        logging.info("Submitting dataset creation request.")
        dataset_request = {
            "name": tar_tdr_dataset_name,
            "description": description,
            "defaultProfileId": tar_tdr_billing_profile,
            "cloudPlatform": tar_tdr_dataset_cloud,
            "region": dataset_region,
            "phsId": phs_id,
            "experimentalSelfHosted": self_hosted,
            "experimentalPredictableFileIds": predictable_file_ids,
            "dedicatedIngestServiceAccount": dedicated_ingest_sa,
            "enableSecureMonitoring": secure_monitoring_enabled,
            "properties": properties,
            "tags": tags,
            "policies": policies,
            "schema": new_schema_dict
        }
        attempt_counter = 1
        while True:
            try:
                create_dataset_result, job_id = wait_for_tdr_job(datasets_api.create_dataset(dataset=dataset_request), tdr_host)
                logging.info("Dataset Creation succeeded: {}".format(create_dataset_result))
                new_dataset_id = create_dataset_result["id"]
                config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Success", create_dataset_result])
                break
            except Exception as e:
                error_str = f"Error on Dataset Creation: {str(e)}"
                logging.error(error_str)
                if attempt_counter < 3:
                    logging.info("Retrying Dataset Creation (attempt #{})...".format(str(attempt_counter)))
                    sleep(10)
                    attempt_counter += 1
                    continue
                else:
                    logging.error("Maximum number of retries exceeded. Exiting job.")
                    config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Failure", error_str])
                    return None, {}, {}
        
    # Exit function
    return new_dataset_id, fileref_col_dict, array_col_dict
    
# Function to create a new TDR dataset from an existing TDR snapshot  
def create_dataset_from_snapshot(config):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    src_tdr_object_env = config["source"]["tdr_object_env"]
    tdr_host = config["source"]["tdr_host"]
    tar_tdr_billing_profile = config["target"]["tdr_billing_profile"]
    tar_tdr_dataset_uuid = config["target"]["tdr_dataset_uuid"]
    tar_tdr_dataset_name = config["target"]["tdr_dataset_name"]
    tar_tdr_dataset_cloud = config["target"]["tdr_dataset_cloud"]
    tar_tdr_dataset_props = config["target"]["tdr_dataset_properties"]
    copy_policies = config["target"]["copy_policies"] 
    apply_anvil_transforms = config["ingest"]["apply_anvil_transforms"]

    # Setup/refresh TDR clients
    api_client = refresh_tdr_api_client(tdr_host)
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
    snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)

    # Retrieve original dataset details
    logging.info(f"Retrieving original {src_tdr_object_type} details from {src_tdr_object_env} environment. UUID:  {src_tdr_object_uuid}")
    try:
        snapshot_details = snapshots_api.retrieve_snapshot(id=src_tdr_object_uuid, include=["TABLES", "RELATIONSHIPS", "ACCESS_INFORMATION", "PROPERTIES", "DATA_PROJECT", "SOURCES"]).to_dict()
    except:
        error_str = f"Error retrieving details from {src_tdr_object_type} {src_tdr_object_uuid} in TDR {src_tdr_object_env} environment: {str(e)}"
        logging.error(error_str)
        config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Failure", error_str])
        return None, {}, {}
    
    # Validate source cloud platform
    config["source"]["tdr_object_cloud"] = snapshot_details["cloud_platform"]
    if config["source"]["tdr_object_cloud"] == "azure":
        config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Failure", "Migrate of Azure TDR objects is not yet supported. Try again with a GCP TDR object."])
        return None, {}, {}
    else:
        config["source"]["bigquery_project"] = snapshot_details["access_information"]["big_query"]["project_id"]
        config["source"]["bigquery_dataset"] = snapshot_details["access_information"]["big_query"]["dataset_name"]

    # Build new dataset schema
    new_schema_dict = {"tables": [], "relationships": [], "assets": []}
    fileref_col_dict = {}
    array_col_dict = {}
    for table_entry in snapshot_details["tables"]:
        int_table_dict = table_entry.copy()
        int_table_dict["primaryKey"] = int_table_dict.pop("primary_key")
        for key in ["partition_mode", "date_partition_options", "int_partition_options", "row_count"]:
            del int_table_dict[key]
        fileref_list = []
        array_list = []
        for idx, column_entry in enumerate(table_entry["columns"]):
            if column_entry["datatype"] == "fileref":
                fileref_list.append(column_entry["name"])
            if column_entry["array_of"] == True:
                array_list.append(column_entry["name"])
            if tar_tdr_dataset_cloud == "azure" and column_entry["datatype"] == "integer":
                table_entry["columns"][idx]["datatype"] = "int64"
        fileref_col_dict[table_entry["name"]] = fileref_list
        array_col_dict[table_entry["name"]] = array_list
        if apply_anvil_transforms:
            if table_entry["name"] == "file_inventory":
                int_table_dict["columns"].append({"name": "orig_file_ref", "datatype": "string", "array_of": False, "required": False})
                int_table_dict["columns"].append({"name": "orig_datarepo_row_id", "datatype": "string", "array_of": False, "required": False})
            elif "anvil_" not in table_entry["name"]:
                int_table_dict["columns"].append({"name": "orig_datarepo_row_id", "datatype": "string", "array_of": False, "required": False})
        new_schema_dict["tables"].append(int_table_dict)
    for rel_entry in snapshot_details["relationships"]:
        int_rel_dict = rel_entry.copy()
        int_rel_dict["from"] = int_rel_dict.pop("_from")
        new_schema_dict["relationships"].append(int_rel_dict)

    # Create a new dataset, unless a target dataset UUID has been provided
    if tar_tdr_dataset_uuid:
        new_dataset_id = tar_tdr_dataset_uuid
        msg_str = f"Attempting to leverage user-provided target dataset UUID ({tar_tdr_dataset_uuid}) rather than creating a new dataset."
        logging.info(msg_str)
        config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Skipped", msg_str])
    else:

        # Determine dataset properties - Description
        orig_object_name = snapshot_details["name"]
        new_description = snapshot_details["description"] + f"\n\nCopy of {src_tdr_object_type} {orig_object_name} from TDR {src_tdr_object_env}."
        description = tar_tdr_dataset_props["description"] if tar_tdr_dataset_props.get("description") else new_description

        # Determine dataset properties - Region
        for storage_entry in snapshot_details["source"][0]["dataset"]["storage"]:
            if storage_entry["cloud_resource"] == "bucket":
                orig_region = storage_entry["region"]
                break
        if tar_tdr_dataset_props.get("region"):
            dataset_region = tar_tdr_dataset_props["region"]
        elif tar_tdr_dataset_cloud == "gcp" and orig_region:
            dataset_region = orig_region
        else:
            dataset_region = None

        # Determine dataset properties - Dedicated Ingest SA
        dedicated_ingest_sa = tar_tdr_dataset_props["dedicatedIngestServiceAccount"] if tar_tdr_dataset_props.get("dedicatedIngestServiceAccount") else False

        # Determine dataset properties - Self-Hosted
        self_hosted = False
        if tar_tdr_dataset_cloud == "azure":
            self_hosted = False
        elif tar_tdr_dataset_props.get("experimentalSelfHosted"):
            self_hosted = tar_tdr_dataset_props["experimentalSelfHosted"]
        else:
            self_hosted = snapshot_details["source"][0]["dataset"]["self_hosted"]
        
        # Determine dataset properties - Policies
        policies = {}
        stewards_list = []
        custodians_list = []
        snapshot_creators_list = []
        if tar_tdr_dataset_props.get("policies"):
            if tar_tdr_dataset_props["policies"].get("stewards"):
                for user in tar_tdr_dataset_props["policies"]["stewards"]:
                    if user not in stewards_list:
                        stewards_list.append(user)
            if tar_tdr_dataset_props["policies"].get("custodians"):
                for user in tar_tdr_dataset_props["policies"]["custodians"]:
                    if user not in custodians_list:
                        custodians_list.append(user)
            if tar_tdr_dataset_props["policies"].get("snapshotCreators"):
                for user in tar_tdr_dataset_props["policies"]["snapshotCreators"]:
                    if user not in snapshot_creators_list:
                        snapshot_creators_list.append(user)
        policies = {
            "stewards": stewards_list,
            "custodians": custodians_list,
            "snapshotCreators": snapshot_creators_list
        }

        # Determine dataset properties - Other
        phs_id = tar_tdr_dataset_props["phsId"] if tar_tdr_dataset_props.get("phsId") else snapshot_details["source"][0]["dataset"]["phs_id"]
        predictable_file_ids = tar_tdr_dataset_props["experimentalPredictableFileIds"] if tar_tdr_dataset_props.get("experimentalPredictableFileIds") else snapshot_details["source"][0]["dataset"]["predictable_file_ids"]
        secure_monitoring_enabled = tar_tdr_dataset_props["enableSecureMonitoring"] if tar_tdr_dataset_props.get("enableSecureMonitoring") else snapshot_details["source"][0]["dataset"]["secure_monitoring_enabled"]
        properties = tar_tdr_dataset_props["properties"] if tar_tdr_dataset_props.get("properties") else snapshot_details["properties"]
        tags = tar_tdr_dataset_props["tags"] if tar_tdr_dataset_props.get("tags") else snapshot_details["tags"]

        # Create new TDR dataset
        logging.info("Submitting dataset creation request.")
        dataset_request = {
            "name": tar_tdr_dataset_name,
            "description": description,
            "defaultProfileId": tar_tdr_billing_profile,
            "cloudPlatform": tar_tdr_dataset_cloud,
            "region": dataset_region,
            "phsId": phs_id,
            "experimentalSelfHosted": self_hosted,
            "experimentalPredictableFileIds": predictable_file_ids,
            "dedicatedIngestServiceAccount": dedicated_ingest_sa,
            "enableSecureMonitoring": secure_monitoring_enabled,
            "properties": properties,
            "tags": tags,
            "policies": policies,
            "schema": new_schema_dict
        }
        attempt_counter = 1
        while True:
            try:
                create_dataset_result, job_id = wait_for_tdr_job(datasets_api.create_dataset(dataset=dataset_request), tdr_host)
                logging.info("Dataset Creation succeeded: {}".format(create_dataset_result))
                new_dataset_id = create_dataset_result["id"]
                config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Success", create_dataset_result])
                break
            except Exception as e:
                error_str = f"Error on Dataset Creation: {str(e)}"
                logging.error(error_str)
                if attempt_counter < 3:
                    logging.info("Retrying Dataset Creation (attempt #{})...".format(str(attempt_counter)))
                    sleep(10)
                    attempt_counter += 1
                    continue
                else:
                    logging.error("Maximum number of retries exceeded. Exiting job.")
                    config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Failure", error_str])
                    return None, {}, {}
    
    # Exit function
    return new_dataset_id, fileref_col_dict, array_col_dict
    
# Function to create a new dataset from an existing TDR object
def create_dataset(config):
    if config["source"]["tdr_object_type"] == "dataset":
        new_dataset_id, fileref_col_dict, array_col_dict = create_dataset_from_dataset(config)
    elif config["source"]["tdr_object_type"] == "snapshot":
        new_dataset_id, fileref_col_dict, array_col_dict = create_dataset_from_snapshot(config) 
    else:
        raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.")
    return new_dataset_id, fileref_col_dict, array_col_dict
        
# Main function to migrate a TDR object
def migrate_object():
    
    # Set up logging
    current_datetime_string = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    logs_stream_file_path = "migration_results_" + current_datetime_string + ".log"
    while logging.root.handlers:
        logging.root.removeHandler(logging.root.handlers[-1])
    logging.basicConfig(format="%(asctime)s - %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO, handlers=[logging.FileHandler(logs_stream_file_path), logging.StreamHandler(sys.stdout)])
    logging.getLogger("azure").setLevel(logging.WARNING)

    # Start pipeline
    logging.info("Starting TDR object migration pipeline.")
    logging.info("Validating input parameters.")

    # Parse arguments and collect configuration
    argParser = create_arg_parser()
    parsedArgs = argParser.parse_args(sys.argv[1:])
    with open(parsedArgs.config_path) as config_file:
        config = json.load(config_file)

    # Validate object type
    if config["source"]["tdr_object_type"] not in ["dataset", "snapshot"]:
        logging.error("Please set source.tdr_object_type to either 'dataset' or 'snapshot'.")
        return

    # Determine Source TDR host based on environment:
    if config["source"]["tdr_object_env"] == "prod":
        config["source"]["tdr_host"] = "https://data.terra.bio"
        config["tdr_general_sa"] = "datarepo-jade-api@terra-datarepo-production.iam.gserviceaccount.com"
    elif config["source"]["tdr_object_env"] == "dev":
        config["source"]["tdr_host"] = "https://jade.datarepo-dev.broadinstitute.org"
        config["tdr_general_sa"] = "jade-k8-sa@broad-jade-dev.iam.gserviceaccount.com"
    else:
        logging.error("Please set source.tdr_object_env to one of 'dev' or 'prod'.")
        return 

    # Setup/refresh TDR clients
    api_client = refresh_tdr_api_client(config["source"]["tdr_host"])
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
    snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
    profiles_api = data_repo_client.ProfilesApi(api_client=api_client)

    # Determine target dataset cloud based on billing profile:
    resp = profiles_api.retrieve_profile(id=config["target"]["tdr_billing_profile"]).to_dict()
    try:
        resp = profiles_api.retrieve_profile(id=config["target"]["tdr_billing_profile"]).to_dict()
        config["target"]["tdr_dataset_cloud"] = resp["cloud_platform"]
    except:
        logging.error("Unable to retrieve the billing profile specified in target.tdr_billing_profile. Please confirm the user has access to this profile.")
        return 

    if config["source"]["tdr_object_type"] == "dataset":
        resp = datasets_api.retrieve_user_dataset_roles(id=config["source"]["tdr_object_uuid"])
    else:
        resp = snapshots_api.retrieve_user_snapshot_roles(id=config["source"]["tdr_object_uuid"])
    if not "steward" in resp:
        logging.error("User has insufficient priviliges on the source TDR object. For both datasets and snapshots, the user must be a 'steward' to use this tool.")
        return  

    # Validate ingest options
    if config["ingest"]["records_fetching_method"] not in ["tdr_api", "cloud_native"]:
        logging.error("Please set ingest.records_fetching_method to either 'tdr_api' or 'cloud_native'.")
        return
    if config["ingest"]["records_processing_method"] not in ["in_memory", "write_to_cloud"]:
        logging.error("Please set ingest.records_processing_method to either 'in_memory' or 'write_to_cloud'.")
        return
    if config["ingest"]["records_processing_method"] == "write_to_cloud" and config["ingest"]["write_to_cloud_platform"] not in ["gcp", "azure"]:
        logging.error("For 'write_to_cloud' records processing method, please set ingest.write_to_cloud_platform to either 'gcp' or 'azure'.")
        return
    if config["ingest"]["records_processing_method"] == "write_to_cloud" and not config["ingest"]["write_to_cloud_location"]:
        logging.error("For 'write_to_cloud' records processing method, please ensure a cloud location is provided in ingest.write_to_cloud_location.")
        return
    if config["ingest"]["records_processing_method"] == "write_to_cloud" and config["ingest"]["write_to_cloud_platform"] == "azure" and not config["ingest"]["write_to_cloud_sas_token"]:
        logging.error("For 'write_to_cloud' records processing method with an ingest.write_to_cloud_platform value of 'azure', please ensure a cloud SAS token is provided in ingest.write_to_cloud_sas_token.")
        return
    if config["ingest"]["records_processing_method"] == "write_to_cloud" and config["ingest"]["write_to_cloud_platform"] != config["target"]["tdr_dataset_cloud"]:
        logging.error("For 'write_to_cloud' records processing method, the ingest.write_to_cloud_platform parameter must have the same value as target.tdr_dataset_cloud.")
        return
    if config["ingest"]["apply_anvil_transforms"] == True and not (config["ingest"]["records_fetching_method"] == "cloud_native" and config["source"]["tdr_object_type"] == "dataset"):
        logging.error("Application of anvil transforms (ingest.apply_anvil_transforms) is only currently supported when source.tdr_object_type is 'dataset' and ingest.records_fetching_method is 'cloud_native'.")
        return

    # Determine record/fileref limits
    if not config["ingest"]["max_records_per_ingest_request"]:
        config["ingest"]["max_records_per_ingest_request"] = 1000000
    if not config["ingest"]["max_filerefs_per_ingest_request"]:
        config["ingest"]["max_filerefs_per_ingest_request"] = 50000 
    elif config["ingest"]["max_filerefs_per_ingest_request"] > 50000:
        logging.warning("Parameter ingest.max_filerefs_per_ingest_request set to value above recommended max of 50000. If errors occur in ingestion, try reducing to below this threshold.")
        
    # Validate snapshot recreation
    if config["snapshot"]["recreate_snapshot"] == True and config["source"]["tdr_object_type"] != "snapshot":
        logging.warning("Parameter snapshot.recreate_snapshot set to false due to source.tdr_object_type not being 'snapshot'.")
        config["snapshot"]["recreate_snapshot"] = False
        
    # Enforce AnVIL restrictions
    if config["ingest"]["apply_anvil_transforms"] == True and config["target"]["tdr_dataset_cloud"] == "azure":
        logging.info("Default region for AnVIL datasets on Azure is 'southcentralus', so setting the target TDR dataset region to this value.")
        config["target"]["tdr_dataset_properties"]["region"] = "southcentralus"

    # Enforce tool/TDR limitations
    if config["target"]["tdr_dataset_cloud"] == "azure":
        logging.info("Dedicated dataset-specific SAs not available for Azure datasets, so setting 'dedicatedIngestServiceAccount' dataset property to False by default.")
        config["target"]["tdr_dataset_properties"]["dedicatedIngestServiceAccount"] = False
        logging.info("Self-hosted functionality not available for Azure datasets, so setting 'experimentalSelfHosted' dataset property to False by default.")
        config["target"]["tdr_dataset_properties"]["experimentalSelfHosted"] = False

    # Validate user role on source object
    logging.info("Validating user permissions.")
    if config["source"]["tdr_object_type"] == "dataset":
        resp = datasets_api.retrieve_user_dataset_roles(id=config["source"]["tdr_object_uuid"])
    else:
        resp = snapshots_api.retrieve_user_snapshot_roles(id=config["source"]["tdr_object_uuid"])
    if not "steward" in resp:
        logging.error("User has insufficient priviliges on the source TDR object. For both datasets and snapshots, the user must be a 'steward' to use this tool.")
        return  

    # Initiate migration pipeline
    config["migration_results"] = []
    logging.info("Starting Dataset Creation step.")
    new_dataset_id, fileref_col_dict, array_col_dict = create_dataset(config)
    if new_dataset_id and fileref_col_dict:
        logging.info("Starting Dataset Ingestion step.")
        populate_new_dataset(config, new_dataset_id, fileref_col_dict, array_col_dict)
    else:
        config["migration_results"].append(["Dataset Ingestion", "All Tables", "Skipped", "Skipped due to upstream failures."])
    if new_dataset_id and config["snapshot"]["recreate_snapshot"]:
        logging.info("Starting Snapshot Creation step.")
        recreate_snapshot(config, new_dataset_id)
    
    # Display migration pipeline results
    pipeline_results = pd.DataFrame(config["migration_results"], columns = ["Task", "Step", "Status", "Message"])
    failures = pipeline_results[pipeline_results["Status"].str.contains("Failure")]
    results_formatted = pprint.pformat(pipeline_results.to_dict('index'), indent=4)
    logging.info("\n-----------------------------------------------------------------------------------------------------\nMigration Pipeline Results:\n-----------------------------------------------------------------------------------------------------\n" + results_formatted)
    logging.info(f"\nPipeline finished with {len(failures)} failures.")

if __name__ == "__main__":
    
    # Migration pipeline execution
    try:
        migrate_object()
    except Exception as e:
        logging.error(f"Error running migration tool to completion: {str(e)}")
