""" 
Migrates a TDR object to a new TDR dataset as configured by the user.

Usage
    > python3 migrate_tdr_object.py -c PATH_TO_CONFIG_FILE

DEPENDENCIES
    > pip install --upgrade data_repo_client

CONFIGURATION
{
    "source": {
        "tdr_object_uuid": "6341ab68-b3ab-4ce5-b9f6-120961cf84b7",
        "tdr_object_type": "snapshot",
        "tdr_object_env": "dev"
    },
    "target": {
        "tdr_billing_profile": "ab050a35-e597-4c81-9d24-331a49e86016",
        "tdr_dataset_uuid": "",
        "tdr_dataset_name": "Migration_Tool_Test",
        "tdr_dataset_cloud": "gcp",
        "tdr_dataset_properties": {},
        "copy_policies": true,
        "tables_to_ingest": [],
        "datarepo_row_ids_to_ingest": []
    },
    "snapshot": {
        "recreate_snapshot": true,
        "new_snapshot_name": "Migration_Tool_Test_SS",
        "copy_snapshot_policies": true
    }
}

"""

# Imports
import data_repo_client
import google.auth
import datetime
import sys
import logging
import argparse
from time import sleep
from google.cloud import bigquery
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
                
# Function to populate new TDR dataset
def populate_new_dataset(config, new_dataset_id, fileref_col_dict):
    # Extract parameters from config
    src_tdr_object_uuid = config["source"]["tdr_object_uuid"]
    src_tdr_object_type = config["source"]["tdr_object_type"]
    src_tdr_object_env = config["source"]["tdr_object_env"]
    tdr_host = config["source"]["tdr_host"]
    tar_tdr_billing_profile = config["target"]["tdr_billing_profile"]
    tdr_general_sa = config["tdr_general_sa"]
    tables_to_ingest = config["target"]["tables_to_ingest"]
    datarepo_row_ids_to_ingest = config["target"]["datarepo_row_ids_to_ingest"]

    # Setup/refresh TDR clients
    api_client = refresh_tdr_api_client(tdr_host)
    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
    snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
    
    # Retrieve TDR SA to add from new dataset and add to original object
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
        config["migration_results"].append(["Dataset Ingestion", "All", "Failure", error_str])
        return
    logging.info(f"TDR SA to add: {tdr_sa_to_use}")
    try:
        if src_tdr_object_type == "dataset":
            resp = datasets_api.add_dataset_policy_member(id=src_tdr_object_uuid, policy_name="steward", policy_member={"email": tdr_sa_to_use}) 
        elif src_tdr_object_type == "snapshot":
            resp = snapshots_api.add_snapshot_policy_member(id=src_tdr_object_uuid, policy_name="steward", policy_member={"email": tdr_sa_to_use}) 
        else:
            raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.")
        logging.info("TDR SA added successfully.")
    except:
        error_str = f"Error adding TDR SA to {src_tdr_object_type} {src_tdr_object_uuid} in TDR {src_tdr_object_env} environment: {str(e)}"
        logging.error(error_str)
        config["migration_results"].append(["Dataset Ingestion", "All", "Failure", error_str])
        return
    
    # Loop through and process tables for ingestion
    logging.info("Processing dataset ingestion requests.")
    ordered_table_list = sorted(fileref_col_dict, key=lambda key: len(fileref_col_dict[key]))
    for table in ordered_table_list:
        
        # Determine whether table should be processed, and skip if not
        if tables_to_ingest and table not in tables_to_ingest:
            msg_str = f"Table '{table}' not listed in the target.tables_to_ingest parameter. Skipping."
            logging.info(msg_str)
            config["migration_results"].append(["Dataset Ingestion", table, "Skipped", msg_str])
            continue
        
        # Retrieve table data from the original dataset
        api_client = refresh_tdr_api_client(tdr_host)
        datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
        snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
        logging.info(f"Fetching records for table '{table}' in the original {src_tdr_object_type} ({src_tdr_object_uuid}).")
        max_page_size = 1000
        total_records_fetched = 0
        final_records = []
        while True:
            row_start = total_records_fetched
            attempt_counter = 0
            while True:
                try:
                    if src_tdr_object_type == "dataset":
                        record_results = datasets_api.lookup_dataset_data_by_id(id=src_tdr_object_uuid, table=table, offset=row_start, limit=max_page_size).to_dict() 
                    elif src_tdr_object_type == "snapshot":
                        record_results = snapshots_api.lookup_snapshot_preview_by_id(id=src_tdr_object_uuid, table=table, offset=row_start, limit=max_page_size).to_dict() 
                    else:
                        raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.")
                    break
                except Exception as e:
                    print(e)
                    if attempt_counter < 5:
                        sleep(10)
                        attempt_counter += 1
                        continue
                    else:
                        record_results = {}
                        break
            if record_results["result"]:
                final_records.extend(record_results["result"])
                total_records_fetched += len(record_results["result"])
            else:
                break
        if final_records:
            df_temp = pd.DataFrame.from_dict(final_records)
            if datarepo_row_ids_to_ingest:
                df_orig = df_temp[df_temp["datarepo_row_id"].isin(datarepo_row_ids_to_ingest)].copy()
            else:
                df_orig = df_temp.copy()
            del df_temp
            df_orig.drop(columns=["datarepo_row_id"], inplace=True, errors="ignore")
            records_orig = df_orig.to_dict(orient="records")
            if len(records_orig) == 0:
                msg_str = f"No records found for table after filtering based on datarepo_row_ids_to_ingest parameter. Continuing to next table."
                logging.info(msg_str)
                config["migration_results"].append(["Dataset Ingestion", table, "Skipped", msg_str])
                continue
            elif len(final_records) != len(records_orig):
                logging.info(f"Filtering records to ingest based on the datarepo_row_ids_to_ingest parameter. {str(len(records_orig))} of {str(len(final_records))} records to be ingested.")
        else:
            msg_str = f"No records found for table in original {src_tdr_object_type}. Continuing to next table."
            logging.info(msg_str)
            config["migration_results"].append(["Dataset Ingestion", table, "Skipped", msg_str])
            continue
        
        # If table contains file references, chunk as appropriate, otherwise ingest as is
        if fileref_col_dict[table]:
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
                                attempt_counter = 0
                                while True:
                                    try:
                                        if src_tdr_object_type == "dataset":
                                            file_results = datasets_api.lookup_file_by_id(id=src_tdr_object_uuid, fileid=val)
                                        elif src_tdr_object_type == "snapshot":
                                            file_results = snapshots_api.lookup_snapshot_file_by_id(id=src_tdr_object_uuid, fileid=val[-36:]) 
                                        else:
                                            raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.") 
                                        fileref_obj = {
                                            "sourcePath": file_results.file_detail.access_url,
                                            "targetPath": file_results.path,
                                            "description": file_results.description,
                                            "mimeType": file_results.file_detail.mime_type
                                        }
                                        fileref_obj_list.append(fileref_obj)
                                        break
                                    except Exception as e:
                                        print(f"Error: {str(e)}")
                                        if attempt_counter < 5:
                                            sleep(5)
                                            attempt_counter += 1
                                            continue
                                        else:
                                            break
                            int_record[fileref_col] = fileref_obj_list
                        elif int_record[fileref_col]:
                            fileref_obj = {}
                            attempt_counter = 0
                            while True:
                                try:
                                    if src_tdr_object_type == "dataset":
                                        file_results = datasets_api.lookup_file_by_id(id=src_tdr_object_uuid, fileid=int_record[fileref_col])
                                    elif src_tdr_object_type == "snapshot":
                                        file_results = snapshots_api.lookup_snapshot_file_by_id(id=src_tdr_object_uuid, fileid=int_record[fileref_col][-36:]) 
                                    else:
                                        raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.") 
                                    fileref_obj = {
                                        "sourcePath": file_results.file_detail.access_url,
                                        "targetPath": file_results.path,
                                        "description": file_results.description,
                                        "mimeType": file_results.file_detail.mime_type
                                    }
                                    int_record[fileref_col] = fileref_obj
                                    break
                                except Exception as e:
                                    print(f"Error: {str(e)}")
                                    if attempt_counter < 5:
                                        sleep(5)
                                        attempt_counter += 1
                                        continue
                                    else:
                                        break
                            int_record[fileref_col] = fileref_obj
                    records_processed.append(int_record)
            except Exception as e:
                err_str = f"Failure in pre-processing: {str(e)}"
                config["migration_results"].append(["Dataset Ingestion", table, "Failure", err_str])
                continue
                
            # Chunk records as necessary and then build, submit, and monitor ingest request(s)
            max_combined_rec_ref_size = 50000
            max_recs_per_request = math.floor(max_combined_rec_ref_size / len(fileref_col_dict[table]))
            records_cnt = len(records_processed)
            start_rec = 0
            end_rec = max_recs_per_request
            while start_rec < records_cnt:
                start_row_str = str(start_rec + 1)
                if end_rec > records_cnt:
                    end_row_str = str(records_cnt)
                else:
                    end_row_str = str(end_rec)
                logging.info(f"Submitting ingestion request for rows {start_row_str}-{end_row_str} (ordered by datarepo_row_id) to new dataset ({new_dataset_id}).")
                ingest_request = {
                    "table": table,
                    "profile_id": tar_tdr_billing_profile,
                    "ignore_unknown_values": True,
                    "resolve_existing_files": True,
                    "updateStrategy": "append",
                    "format": "array",
                    "load_tag": "Ingest for {}".format(new_dataset_id),
                    "records": records_processed[start_rec:end_rec]
                }
                attempt_counter = 1
                while True:
                    try:
                        api_client = refresh_tdr_api_client(tdr_host)
                        datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
                        ingest_request_result, job_id = wait_for_tdr_job(datasets_api.ingest_dataset(id=new_dataset_id, ingest=ingest_request), tdr_host)
                        logging.info("Ingest succeeded: {}".format(str(ingest_request_result)[0:1000]))
                        config["migration_results"].append(["Dataset Ingestion", table + f" (rows {start_row_str}-{end_row_str})", "Success", str(ingest_request_result)[0:1000]])
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
                            config["migration_results"].append(["Dataset Ingestion", table + f" (rows {start_row_str}-{end_row_str})", "Failure", err_str])
                            break
                start_rec += max_recs_per_request
                if (end_rec + max_recs_per_request) > records_cnt:
                    end_rec = records_cnt + 1
                else:
                    end_rec += max_recs_per_request 
        else:
            # Build, submit, and monitor ingest request
            logging.info(f"Submitting ingestion request to new dataset ({new_dataset_id}).")
            ingest_request = {
                "table": table,
                "profile_id": tar_tdr_billing_profile,
                "ignore_unknown_values": True,
                "resolve_existing_files": True,
                "updateStrategy": "append",
                "format": "array",
                "load_tag": "Ingest for {}".format(new_dataset_id),
                "records": records_orig
            }
            attempt_counter = 1
            while True:
                try:
                    api_client = refresh_tdr_api_client(tdr_host)
                    datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
                    ingest_request_result, job_id = wait_for_tdr_job(datasets_api.ingest_dataset(id=new_dataset_id, ingest=ingest_request), tdr_host)
                    logging.info("Ingest succeeded: {}".format(str(ingest_request_result)[0:1000]))
                    config["migration_results"].append(["Dataset Ingestion", table, "Success", str(ingest_request_result)[0:1000]])
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
                        config["migration_results"].append(["Dataset Ingestion", table + f" (rows {start_row_str}-{end_row_str})", "Failure", err_str])  
                        break

# Function to create a new TDR dataset from an existing TDR dataset
def create_dataset_from_dataset(config):
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
        return None, {}
    
    # Validate source cloud platform
    for storage_entry in dataset_details["storage"]:
        if storage_entry["cloud_platform"] == "azure":
            config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Failure", "Migrate of Azure TDR objects is not yet supported. Try again with a GCP TDR object."])
            return None, {}

    # Build new dataset schema
    new_schema_dict = {"tables": [], "relationships": [], "assets": []}
    fileref_col_dict = {}
    for table_entry in dataset_details["schema"]["tables"]:
        int_table_dict = table_entry.copy()
        int_table_dict["primaryKey"] = int_table_dict.pop("primary_key")
        for key in ["partition_mode", "date_partition_options", "int_partition_options", "row_count"]:
            del int_table_dict[key]
        new_schema_dict["tables"].append(int_table_dict)
        fileref_list = []
        for column_entry in table_entry["columns"]:
            if column_entry["datatype"] == "fileref":
                fileref_list.append(column_entry["name"])
        fileref_col_dict[table_entry["name"]] = fileref_list
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
        new_description = f"Copy of {src_tdr_object_type} {orig_object_name} from TDR {src_tdr_object_env}. Original description below:\n\n" + dataset_details["description"]
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
            dedicated_ingest_sa = True if dataset_details["ingest_service_account"] else False

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
                    return None, {}
        
    # Exit function
    return new_dataset_id, fileref_col_dict
    
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
        return None, {}
    
    # Validate source cloud platform
    for storage_entry in snapshot_details["source"][0]["dataset"]["storage"]:
        if storage_entry["cloud_platform"] == "azure":
            config["migration_results"].append(["Dataset Creation", "Dataset Creation", "Failure", "Migrate of Azure TDR objects is not yet supported. Try again with a GCP TDR object."])
            return None, {}

    # Build new dataset schema
    new_schema_dict = {"tables": [], "relationships": [], "assets": []}
    fileref_col_dict = {}
    for table_entry in snapshot_details["tables"]:
        int_table_dict = table_entry.copy()
        int_table_dict["primaryKey"] = int_table_dict.pop("primary_key")
        for key in ["partition_mode", "date_partition_options", "int_partition_options", "row_count"]:
            del int_table_dict[key]
        new_schema_dict["tables"].append(int_table_dict)
        fileref_list = []
        for column_entry in table_entry["columns"]:
            if column_entry["datatype"] == "fileref":
                fileref_list.append(column_entry["name"])
        fileref_col_dict[table_entry["name"]] = fileref_list
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
        new_description = f"Copy of {src_tdr_object_type} {orig_object_name} from TDR {src_tdr_object_env}. Original description below:\n\n" + snapshot_details["description"]
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
                    return None, {}
    
    # Exit function
    return new_dataset_id, fileref_col_dict
    
# Function to create a new dataset from an existing TDR object
def create_dataset(config):
    if config["source"]["tdr_object_type"] == "dataset":
        new_dataset_id, ordered_ingest_list = create_dataset_from_dataset(config)
    elif config["source"]["tdr_object_type"] == "snapshot":
        new_dataset_id, ordered_ingest_list = create_dataset_from_snapshot(config) 
    else:
        raise Exception("Source TDR object type must be 'dataset' or 'snapshot'.")
    return new_dataset_id, ordered_ingest_list
        
# Main function to migrate a TDR object
def migrate_object():
    
    # Parse arguments and collect configuration
    argParser = create_arg_parser()
    parsedArgs = argParser.parse_args(sys.argv[1:])
    with open(parsedArgs.config_path) as config_file:
        config = json.load(config_file)
    
    # Validate object type
    logging.info("Starting TDR object migration pipeline for {obj_type} {obj} in {env} environment.".format(obj_type=config["source"]["tdr_object_type"], obj=config["source"]["tdr_object_uuid"], env=config["source"]["tdr_object_env"]))
    logging.info("Validating input parameters.")
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
        
    # Validate Target Dataset Cloud
    if config["target"]["tdr_dataset_cloud"] not in ["gcp", "azure"]:
        logging.error("Please set target.tdr_dataset_cloud to either 'gcp' or 'azure'.")
        return
        
    # Validate snapshot recreation
    if config["snapshot"]["recreate_snapshot"] == True and config["source"]["tdr_object_type"] != "snapshot":
        logging.warning("Parameter snapshot.recreate_snapshot set to false due to source.tdr_object_type not being 'snapshot'.")
        config["snapshot"]["recreate_snapshot"] = False

    # Enforce tool/TDR limitations
    logging.info("Dedicated dataset-specific SAs not currently supported for TDR-to-TDR ingestions, so setting 'dedicatedIngestServiceAccount' dataset property to False by default.")
    config["target"]["tdr_dataset_properties"]["dedicatedIngestServiceAccount"] = False
    if config["target"]["tdr_dataset_cloud"] == "azure":
        logging.info("Self-hosted functionality not available for Azure datasets, so setting 'experimentalSelfHosted' dataset property to False by default.")
        config["target"]["tdr_dataset_properties"]["experimentalSelfHosted"] = False

    # Initiate migration pipeline
    config["migration_results"] = []
    logging.info("Starting Dataset Creation step.")
    new_dataset_id, fileref_col_dict = create_dataset(config)
    if new_dataset_id and fileref_col_dict:
        logging.info("Starting Dataset Ingestion step.")
        populate_new_dataset(config, new_dataset_id, fileref_col_dict)
    else:
        config["migration_results"].append(["Dataset Ingestion", "All", "Skipped", "Skipped due to upstream failures."])
    if new_dataset_id and config["snapshot"]["recreate_snapshot"]:
        logging.info("Starting Snapshot Creation step.")
        recreate_snapshot(config, new_dataset_id)
    
    # Display migration pipeline results
    pipeline_results = pd.DataFrame(config["migration_results"], columns = ["Task", "Step", "Status", "Message"])
    results_formatted = pprint.pformat(pipeline_results.to_dict('index'), indent=4)
    logging.info("\n-----------------------------------------------------------------------------------------------------\nMigration Pipeline Results:\n-----------------------------------------------------------------------------------------------------\n" + results_formatted)

if __name__ == "__main__":
    
    # Logging configuration
    while logging.root.handlers:
        logging.root.removeHandler(logging.root.handlers[-1])
    current_datetime = datetime.datetime.now()
    current_datetime_string = current_datetime.strftime("%Y%m%d%H%M")
    logging.basicConfig(format="%(asctime)s - %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO, handlers=[logging.FileHandler(f"migration_results_{current_datetime_string}.log"), logging.StreamHandler(sys.stdout)])

    # Migration pipeline execution
    try:
        migrate_object()
    except Exception as e:
        logging.error(f"Error running migration tool to completion: {str(e)}")
