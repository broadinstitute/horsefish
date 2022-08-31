# USAGE: python3 validate_tdr_data.py "UUID" --storageType "snapshot" --env "prod" --schemaFilePath "Path" --outputDirectory "Path"
# DEPENDENCIES: 
#     - google-cloud-bigquery < 3.0.0 (https://github.com/feast-dev/feast/issues/2537)

# Imports and configuration
import sys
import os
import logging
import argparse
import pandas as pd
import data_repo_client
import google.auth
import google.auth.transport.requests
from google.cloud import bigquery
import uuid
import json
import datetime

# Function to create argument parser
def create_arg_parser():
    # Define arguments to be collected by the script
    parser = argparse.ArgumentParser(description = "This script is used to profile and validate datasets stored in the Terra Data Repository.")
    parser.add_argument("uuid", help = "The UUID of the TDR dataset or snapshot to be validated.")
    parser.add_argument("--storageType", help = "Optional parameter to specify whether the data to be validated lives in a 'snapshot' or a 'dataset' (by passing one of those values into the parameter). If unspecified, the script will assume the data is in a dataset.")
    parser.add_argument("--env", help = "Optional parameter to specify which TDR environment the data to be validated lives in. Use 'prod' to specify production or 'dev' to specify development. If unspecified, the script will assume the data is in production.")
    parser.add_argument("--schemaFilePath", help = "Optional parameter to specify the relative path to a JSON schema definition file to compare against the schema being used for the data in TDR. This file should contain 'tables' and 'relationships' properties and be formatted like the TDR schema definition object. If unspecified, the schema comparison checks will be skipped.")
    parser.add_argument("--outputDirectory", help = "Optional parameter to specify the relative path to the directory where the results and log files should be written. If unspecified, these file will be created in the directory the script is run from.")
    return parser

# Function to validate UUID provided is value
def is_valid_uuid(value):
    try:
        uuid.UUID(str(value))
        return True
    except ValueError:
        return False

# Function to validate a specified file exists
def file_exists(filename):
    try:
        f = open(os.path.expanduser(filename))
        f.close()
        return True
    except IOError:
        return False

# Function to retrieve the TDR schema definition for the data
def retrieve_tdr_schema(uuid, storage_type, api_client):
    # Retrieve TDR schema definition from a dataset or snapshot
    tdr_schema_dict = {}
    bq_project = ""
    bq_schema = ""
    skip_bq_queries = False
    try:
        if storage_type == "dataset":
            datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
            response = datasets_api.retrieve_dataset(id=uuid, include=["SCHEMA", "ACCESS_INFORMATION"]).to_dict()
            tdr_schema_dict["tables"] = response["schema"]["tables"]
            tdr_schema_dict["relationships"] = response["schema"]["relationships"]
            try:
                bq_project = response["access_information"]["big_query"]["project_id"]
                bq_schema = response["access_information"]["big_query"]["dataset_name"]
            except:
                logging.error("Error retrieving BigQuery Project and Dataset information. Skipping BQ-based data profiling checks. Confirm this is a BigQuery hosted dataset and try again to run these checks.")
                skip_bq_queries = True
        else:
            snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)
            response = snapshots_api.retrieve_snapshot(id=uuid, include=["TABLES", "RELATIONSHIPS", "ACCESS_INFORMATION"]).to_dict()
            tdr_schema_dict["tables"] = response["tables"]
            tdr_schema_dict["relationships"] = response["relationships"]
            try:
                bq_project = response["access_information"]["big_query"]["project_id"]
                bq_schema = response["access_information"]["big_query"]["dataset_name"]
            except:
                logging.error("Error retrieving BigQuery Project and Dataset information. Skipping BQ-based data profiling  checks. Confirm this is a BigQuery hosted snapshot and try again to run these checks.")
                skip_bq_queries = True
    except Exception as e:
        logging.error("TDR error on retrieving specified dataset: {}".format(str(e)))
        logging.error("Exiting script.")
        sys.exit(1)
    return tdr_schema_dict, bq_project, bq_schema, skip_bq_queries

# Function to retrieve a TDR schema definition and parse it into more useful objects
def process_tdr_schema(tdr_schema_dict):
    # Parse TDR schema into a table set, array field set, and field list for use in query construction
    table_set = set()
    array_field_set = set()
    field_list = []
    relationship_count = len(tdr_schema_dict["relationships"])
    for table_entry in tdr_schema_dict["tables"]:
        table_set.add(table_entry["name"])
        for column_entry in table_entry["columns"]:
            field_dict = {}
            field_dict["table"] = table_entry["name"]
            field_dict["column"] = column_entry["name"]
            field_dict["datatype"] = column_entry["datatype"]
            field_dict["is_array"] = column_entry["array_of"]
            field_dict["required"] = column_entry["required"]
            if column_entry["name"] in table_entry["primary_key"]:
                field_dict["is_primary_key"] = True
            else:
                field_dict["is_primary_key"] = False
            joins_to_list = []
            for relation_entry in tdr_schema_dict["relationships"]:
                joins_to_dict = {}
                if relation_entry["_from"]["table"] == table_entry["name"] and relation_entry["_from"]["column"] == column_entry["name"]:
                    joins_to_dict["table"] = relation_entry["to"]["table"]
                    joins_to_dict["column"] = relation_entry["to"]["column"]
                    joins_to_list.append(joins_to_dict)
            field_dict["joins_to"] = joins_to_list
            joins_from_list = []
            for relation_entry in tdr_schema_dict["relationships"]:
                joins_from_dict = {}
                if relation_entry["to"]["table"] == table_entry["name"] and relation_entry["to"]["column"] == column_entry["name"]:
                    joins_from_dict["table"] = relation_entry["_from"]["table"]
                    joins_from_dict["column"] = relation_entry["_from"]["column"]
                    joins_from_list.append(joins_from_dict)
            field_dict["joins_from"] = joins_from_list
            field_list.append(field_dict)
            if column_entry["array_of"] == True:
                array_field_set.add(table_entry["name"] + "." + column_entry["name"])
    return table_set, array_field_set, field_list, relationship_count 

# Function to collect table level statistics: row counts
def run_table_profiling_checks(client, df, bq_project, bq_schema, table_set, field_list):
    logging.info("Building and executing table-level queries...")
    # Loop through tables in the table set and pull record counts (and record empty tables for use in column-level queries)
    empty_table_list = []
    query_count = 0
    for table_entry in table_set:

        # Construct the table record count query
        row_count_query = """SELECT 'Summary Stats' AS metric_type, '{table}' AS source_table, 'All' AS source_column, 
                   'Count of records in table' AS metric, 
                   COUNT(*) AS n, null AS d, null AS r, 
                   CASE WHEN COUNT(*) = 0 THEN 1 END AS flag 
                   FROM `{project}.{schema}.{table}`""".format(project = bq_project, schema = bq_schema, table = table_entry)

        # Execute the query and append results to dataframe
        query_count += 1
        #print(row_count_query)
        try:
            df_temp = client.query(row_count_query).result().to_dataframe()
            df = df.append(df_temp)
            if df_temp["n"].values[0] == 0:
                empty_table_list.append(table_entry)
            else:

                # Identify all fileref columns
                fileref_col_list = []
                for column_entry in field_list:
                    if column_entry["table"] == table_entry and column_entry["datatype"] == "fileref":
                        fileref_col_list.append("'" + column_entry["column"] + "'")
                if len(fileref_col_list) > 0:
                    fileref_col_str = ", ".join(fileref_col_list)
                else:
                    fileref_col_str = "''"

                # Construct the null column count query
                null_query = """WITH null_counts AS
                        (
                          SELECT column_name, COUNT(1) AS cnt
                          FROM `{project}.{schema}.{table}`, 
                          UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(`{project}.{schema}.{table}`), r'"(\w+)":(?:null|\[\])')) column_name
                          GROUP BY column_name
                        ),
                        table_count AS
                        (
                          SELECT COUNT(*) AS cnt FROM `{project}.{schema}.{table}`
                        )
                        SELECT 'Summary Stats' AS metric_type, src.table_name AS source_table, src.column_name AS source_column, 
                        'Count of nulls or empty lists in column'||CASE WHEN src.column_name IN ({fileref_list}) THEN ' (fileref)' ELSE '' END AS metric,
                        COALESCE(tar.cnt, 0) AS n, 
                        table_count.cnt AS d,
                        CASE WHEN table_count.cnt > 0 THEN COALESCE(tar.cnt, 0)/table_count.cnt END AS r,
                        CASE WHEN COALESCE(tar.cnt, 0) > 0 AND src.column_name IN ({fileref_list}) THEN 1 END AS flag
                        FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS` src
                          LEFT JOIN null_counts tar ON src.column_name = tar.column_name
                          CROSS JOIN table_count
                        WHERE src.table_name = '{table}'
                        AND src.column_name NOT IN ('datarepo_row_id', 'datarepo_ingest_date')""".format(project = bq_project, schema = bq_schema, table = table_entry, fileref_list = fileref_col_str)

                # Execute the null count query and append results to dataframe
                query_count += 1
                #print(null_query)
                try:
                    df = df.append(client.query(null_query).result().to_dataframe())
                except Exception as e:
                    logging.error("Error during query execution: {}".format(str(e)))
                
                # Construct the distinct column value query
                distinct_query = """WITH distinct_counts AS
                        (
                          SELECT column_name, APPROX_COUNT_DISTINCT(CASE WHEN column_value NOT IN ('null', '[]') THEN column_value END) AS cnt
                          FROM `{project}.{schema}.{table}`,
                          UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(`{project}.{schema}.{table}`), r'"(\w+)":')) AS column_name WITH OFFSET pos1,
                          UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(`{project}.{schema}.{table}`), r':(.+?),')) AS column_value WITH OFFSET pos2
                          WHERE pos1 = pos2
                          GROUP BY column_name
                        ),
                        table_count AS
                        (
                          SELECT COUNT(*) AS cnt FROM `{project}.{schema}.{table}`
                        )
                        SELECT 'Summary Stats' AS metric_type, src.table_name AS source_table, src.column_name AS source_column, 
                        'Count of distinct values in column' AS metric,
                        COALESCE(tar.cnt, 0) AS n, 
                        table_count.cnt AS d,
                        CASE WHEN table_count.cnt > 0 THEN COALESCE(tar.cnt, 0)/table_count.cnt END AS r,
                        null AS flag
                        FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS` src
                          LEFT JOIN distinct_counts tar ON src.column_name = tar.column_name
                          CROSS JOIN table_count
                        WHERE src.table_name = '{table}'
                        AND src.column_name NOT IN ('datarepo_row_id', 'datarepo_ingest_date')""".format(project = bq_project, schema = bq_schema, table = table_entry)
                                        
                # Execute the distinct count query and append results to dataframe
                query_count += 1
                #print(distinct_query)  
                try:
                    df = df.append(client.query(distinct_query).result().to_dataframe())
                except Exception as e:
                    logging.error("Error during query execution: {}".format(str(e)))
        except Exception as e:
            logging.error("Error during query execution: {}".format(str(e)))

    logging.info("Table-level queries complete. {0} queries executed.".format(query_count))
    return df, empty_table_list

# Function to collect column level statistics: null counts, unique counts, linkage counts (counts of records where foreign key doesn"t join to a primary key), and reverse linkage counts (counts of records where a primary key isn"t reference by any foriegn key)
def run_column_profiling_checks(client, df, bq_project, bq_schema, field_list, array_field_set, empty_table_list):
    logging.info("Building and executing column-level queries...")
    # Loop through columns and pull null counts and distinct value counts
    query_count = 0
    for column_entry in field_list:
    
        # Skip column-level queries for tables that don't have records (to save processing time)
        if column_entry["table"] not in empty_table_list:
    
            # Loop through join_to fields (if any) and build linkage queries
            table_name = column_entry["table"]
            col_name = column_entry["column"]
            for join_entry in column_entry["joins_to"]:

                # Set parameters for linkage queries
                target_table = join_entry["table"]
                target_col = join_entry["column"]
                target_table_col = target_table + "." + target_col
                if column_entry["is_array"] == True:
                    src_col_name = "{col}_unnest".format(col = col_name)
                    from_statement = "(select * from `{project}.{schema}.{table}` t left join unnest(t.{col}) as {unnest_col}) src".format(project = bq_project, schema = bq_schema, table = table_name, col = col_name, unnest_col = src_col_name)
                    where_statement = "array_length(src.{col}) > 0".format(col = col_name)
                else:
                    src_col_name = col_name
                    from_statement = "`{project}.{schema}.{table}` src".format(project = bq_project, schema = bq_schema, table = table_name)
                    where_statement = "src.{col} is not null".format(col = col_name)
                if target_table_col in array_field_set:
                    tar_col_name = "{col}_unnest".format(col = target_col)
                    join_statement = "(select * from `{project}.{schema}.{table}` t left join unnest(t.{col}) as {unnest_col}) tar".format(project = bq_project, schema = bq_schema, table = target_table, col = target_col, unnest_col = tar_col_name)
                else:
                    tar_col_name = target_col
                    join_statement = "`{project}.{schema}.{table}` tar".format(project = bq_project, schema = bq_schema, table = target_table)

                # Construct the linkage query
                linkage_query = """SELECT 'Referential Integrity' AS metric_type, '{table}' AS source_table, '{col}' AS source_column, 
                       'Count of non-null rows that do not fully join to {target}' AS metric, 
                       COUNT(DISTINCT CASE WHEN tar.datarepo_row_id IS NULL THEN src.datarepo_row_id END) AS n, 
                       COUNT(DISTINCT src.datarepo_row_id) AS d, 
                       CASE WHEN COUNT(DISTINCT src.datarepo_row_id) > 0 THEN COUNT(DISTINCT CASE WHEN tar.datarepo_row_id IS NULL THEN src.datarepo_row_id END)/COUNT(DISTINCT src.datarepo_row_id) END AS r, 
                       CASE WHEN COUNT(DISTINCT CASE WHEN tar.datarepo_row_id IS NULL THEN src.datarepo_row_id END) > 0 THEN 1 END AS flag
                       FROM {frm}
                       LEFT JOIN {join}
                       ON src.{src_col} = tar.{tar_col}
                       WHERE {where}""".format(project = bq_project, schema = bq_schema, table = table_name, col = col_name, target = target_table_col, frm = from_statement, join = join_statement, src_col = src_col_name, tar_col = tar_col_name, where = where_statement)

                # Execute the referential integrity query and append results to dataframe
                query_count += 1
                #print(linkage_query)
                try:
                    df = df.append(client.query(linkage_query).result().to_dataframe())
                except Exception as e:
                    logging.error("Error during query execution: {}".format(str(e)))

            # For primary key fields, loop through join_from fields and build reverse linkage checks
            if column_entry["is_primary_key"] == True and len(column_entry["joins_from"]) > 0:

                # Construct CTE that includes all foreign keys that reference the primary key
                counter = 0
                cte_query = "WITH temp_fks AS ("
                source_col_list = []
                for entry in column_entry["joins_from"]:
                    cte_query_segment = ""
                    counter += 1
                    source_table = entry["table"]
                    source_column = entry["column"]
                    source_table_col = entry["table"] + "." + entry["column"]
                    source_col_list.append(source_table_col)
                    if counter > 1:
                        cte_query_segment = "UNION ALL "
                    if source_table_col in array_field_set:
                        cte_query_segment += "SELECT DISTINCT {tar_col} FROM `{project}.{schema}.{table}` CROSS JOIN UNNEST({src_col}) AS {tar_col}".format(project = bq_project, schema = bq_schema, table = source_table, src_col = source_column, tar_col = col_name)
                    else:
                        cte_query_segment += "SELECT DISTINCT {src_col} as {tar_col}  FROM `{project}.{schema}.{table}`".format(project = bq_project, schema = bq_schema, table = source_table, src_col = source_column, tar_col = col_name)
                    cte_query = cte_query + cte_query_segment + " "
                cte_query = cte_query + ")"
                source_col_list_string = ", ".join(source_col_list)

                # Construct the reverse linkage query
                reverse_linkage_query = """{cte} SELECT 'Referential Integrity' As metric_type, '{table}' AS source_table, '{col}' AS source_column, 
                                      'Count of rows where primary key is not referenced by foreign key fields ({fk_list})' AS metric,
                                      COUNT(DISTINCT CASE WHEN tar.{col} IS NULL THEN src.{col} END) AS n,
                                      COUNT(DISTINCT src.{col}) AS d,
                                      CASE WHEN COUNT(DISTINCT src.{col}) > 0 THEN COUNT(DISTINCT CASE WHEN tar.{col} IS NULL THEN src.{col} END)/COUNT(DISTINCT src.{col}) END AS r, 
                                      CASE WHEN COUNT(DISTINCT CASE WHEN tar.{col} IS NULL THEN src.{col} END) > 0 THEN 1 END AS flag
                                      FROM `{project}.{schema}.{table}` src LEFT JOIN temp_fks tar ON src.{col} = tar.{col}""".format(cte = cte_query, project = bq_project, schema = bq_schema, table = table_name, col = col_name, fk_list = source_col_list_string)

                # Execute the reverse linkage query and append results to dataframe
                query_count += 1
                #print(reverse_linkage_query)
                try:
                    df = df.append(client.query(reverse_linkage_query).result().to_dataframe())
                except Exception as e:
                    logging.error("Error during query execution: {}".format(str(e)))
                
    logging.info("Column-level queries complete. {0} queries executed.".format(query_count))
    return df

# Function to collect the files in TDR that aren't referenced in the table data
def run_orphan_file_checks(client, df, bq_project, bq_schema, field_list, array_field_set):
    logging.info("Building and executing orphaned files query...")
    # Collect file reference fields
    file_ref_list = []
    for column_entry in field_list:
        column_dict = {}
        if column_entry["datatype"] == "fileref":
            column_dict["table"] = column_entry["table"]
            column_dict["column"] = column_entry["column"]
            file_ref_list.append(column_dict)

    # Construct CTE that includes all fileref fields
    counter = 0
    orphan_count = 0
    cte_query = "WITH temp_fks AS ("
    source_col_list = []
    if len(file_ref_list) > 0:
        for entry in file_ref_list:
            cte_query_segment = ""
            counter += 1
            source_table = entry["table"]
            source_column = entry["column"]
            source_table_col = entry["table"] + "." + entry["column"]
            source_col_list.append(source_table_col)
            if counter > 1:
                cte_query_segment = "UNION ALL "
            if source_table_col in array_field_set:
                cte_query_segment += "SELECT DISTINCT file_id FROM `{project}.{schema}.{table}` CROSS JOIN UNNEST({src_col}) AS file_id".format(project = bq_project, schema = bq_schema, table = source_table, src_col = source_column)
            else:
                cte_query_segment += "SELECT DISTINCT {src_col} AS file_id  FROM `{project}.{schema}.{table}`".format(project = bq_project, schema = bq_schema, table = source_table, src_col = source_column)
            cte_query = cte_query + cte_query_segment + " "
            source_col_list_string = ", ".join(source_col_list)
    else:
        cte_query += "SELECT '1' AS file_id" 
        source_col_list_string = ""
    cte_query += ")"

    # Construct the orphaned files query
    orphaned_file_query = """{cte} SELECT 'Orphaned Files' As metric_type, 'datarepo_load_history' AS source_table, 'file_id' AS source_column, 
                          'Count of file_ids not referenced by a fileref field ({fk_list})' AS metric,
                          COUNT(DISTINCT CASE WHEN tar.file_id IS NULL THEN src.file_id END) AS n,
                          COUNT(DISTINCT src.file_id) AS d,
                          CASE WHEN COUNT(DISTINCT src.file_id) > 0 THEN COUNT(DISTINCT CASE WHEN tar.file_id IS NULL THEN src.file_id END)/COUNT(DISTINCT src.file_id) END AS r, 
                          CASE WHEN CASE WHEN COUNT(DISTINCT src.file_id) > 0 THEN COUNT(DISTINCT CASE WHEN tar.file_id IS NULL THEN src.file_id END)/COUNT(DISTINCT src.file_id) END > 0 THEN 1 END AS flag
                          FROM `{project}.{schema}.datarepo_load_history` src LEFT JOIN temp_fks tar ON src.file_id = tar.file_id
                          WHERE state = 'succeeded'""".format(cte = cte_query, project = bq_project, schema = bq_schema, fk_list = source_col_list_string)

    # Execute the orphaned files query and append results to dataframe
    #print(orphaned_file_query)
    try:
        df_temp = client.query(orphaned_file_query).result().to_dataframe()
        df = df.append(df_temp)
        orphan_count = df_temp["n"].values[0]
    except Exception as e:
        logging.error("Error during query execution: {}".format(str(e)))
    
    logging.info("Orphaned file query complete. {0} orphaned files found.".format(orphan_count))
    return df

# Function to compare the TDR schema definition with the referenced schema definition
def run_schema_comparison_checks(df, tdr_schema_dict, comparison_schema):
    logging.info("Executing schema comparison checks...")
    result_list = []
    # Table existence comparison
    tdr_table_set = set()
    comp_table_set = set()
    #disjunctive_table_set = set()
    for table_entry in tdr_schema_dict["tables"]:
        tdr_table_set.add(table_entry["name"])
    try:
        for table_entry in comparison_schema["tables"]:
            comp_table_set.add(table_entry["name"])
    except KeyError:
        logging.error("Comparison schema file 'tables' property is missing or malformed. Will skip remaining schema comparison checks.")
        return
    in_tdr_not_comp = tdr_table_set.difference(comp_table_set)
    for item in in_tdr_not_comp:
        result_list.append(["Schema Comparison", item, "All", "In TDR schema but not comparison schema", 0, 0, 0, 0])
    in_comp_not_tdr = comp_table_set.difference(tdr_table_set)
    for item in in_comp_not_tdr:
        result_list.append(["Schema Comparison", item, "All", "In comparison schema but not TDR schema", 0, 0, 0, 1])
    disjunctive_table_set = in_tdr_not_comp.union(in_comp_not_tdr)
    logging.info("Table comparison results: \n Count tables present in TDR schema but not comparison schema file: {0} \n Count tables present in comparison schema file but not TDR schema: {1}".format(len(in_tdr_not_comp), len(in_comp_not_tdr)))
    
    # Column existence comparison
    tdr_column_set = set()
    comp_column_set = set()
    for table_entry in tdr_schema_dict["tables"]:
        if table_entry["name"] not in disjunctive_table_set:
            for column_entry in table_entry["columns"]:
                tdr_column_set.add(table_entry["name"] + " - " + column_entry["name"])
    try:
        for table_entry in comparison_schema["tables"]:
            if table_entry["name"] not in disjunctive_table_set:
                for column_entry in table_entry["columns"]:
                    comp_column_set.add(table_entry["name"] + " - " + column_entry["name"]) 
    except KeyError:
        logging.error("Comparison schema file 'tables' property is missing or malformed. Will skip remaining schema comparison checks.")
        return
    in_tdr_not_comp = tdr_column_set.difference(comp_column_set)
    for item in in_tdr_not_comp:
        result_list.append(["Schema Comparison", item.split(" - ")[0], item.split(" - ")[1], "In TDR schema but not comparison schema", 0, 0, 0, 0])
    in_comp_not_tdr = comp_column_set.difference(tdr_column_set)
    for item in in_comp_not_tdr:
        result_list.append(["Schema Comparison", item.split(" - ")[0], item.split(" - ")[1], "In comparison schema but not TDR schema", 0, 0, 0, 1])  
    logging.info("Column comparison results for tables present in both schemas: \n Count columns present in TDR schema but not comparison schema file: {0} \n Count columns present in comparison schema file but not TDR schema: {1}".format(len(in_tdr_not_comp), len(in_comp_not_tdr)))
    
    # Column attribute differences
    column_diff_set = set()
    try:
        for table_entry in comparison_schema["tables"]:
            for column_entry in table_entry["columns"]:
                for tdr_table_entry in tdr_schema_dict["tables"]:
                    if tdr_table_entry["name"] == table_entry["name"]:
                        for tdr_column_entry in tdr_table_entry["columns"]:
                            if tdr_column_entry["name"] == column_entry["name"]:
                                diff_attr_list = []
                                # Compare "datatype" attribute
                                if tdr_column_entry["datatype"] != column_entry["datatype"]:
                                    diff_attr_list.append("datatype")
                                
                                # Compare "array_of" attribute
                                try:
                                    comp_array_of = column_entry["array_of"]
                                except KeyError:
                                    comp_array_of = False
                                try:
                                    tdr_array_of = tdr_column_entry["array_of"]
                                except KeyError:
                                    tdr_array_of = False
                                if comp_array_of != tdr_array_of:
                                    diff_attr_list.append("array_of")
                                
                                # Compare "required" attribute
                                try:
                                    comp_required = column_entry["required"]
                                except KeyError:
                                    comp_required = False
                                try:
                                    tdr_required = tdr_column_entry["required"]
                                except KeyError:
                                    tdr_required = False
                                if comp_required != tdr_required:
                                    diff_attr_list.append("required")
                                
                                # Add column differences to column_diff_set
                                if len(diff_attr_list) > 0:
                                    diff_attr_str = ','.join(diff_attr_list)
                                    column_diff_set.add(table_entry["name"] + " - " + column_entry["name"] + " - " + diff_attr_str)
    except KeyError:
        logging.error("Comparison schema file 'tables' property is missing or malformed. Will skip remaining schema comparison checks.")
        return
    for item in column_diff_set:
        result_list.append(["Schema Comparison", item.split(" - ")[0], item.split(" - ")[1], "Difference in attributes of shared column (" + item.split(" - ")[2] + ")", 0, 0, 0, 1])  
    logging.info("Column attribute comparison results for columns present in both schemas: \n Count columns with differing attributes between TDR schema and comparison schema file: {0}".format(len(column_diff_set)))
    
    # Relationship existence comparison
    tdr_relationship_set = set()
    comp_relationship_set = set()
    for rel_entry in tdr_schema_dict["relationships"]:
        tdr_relationship_set.add(rel_entry["_from"]["table"] + " - " + rel_entry["_from"]["column"] + " - " + rel_entry["to"]["table"] + " - " + rel_entry["to"]["column"])
    try:
        for rel_entry in comparison_schema["relationships"]:
            comp_relationship_set.add(rel_entry["from"]["table"] + " - " + rel_entry["from"]["column"] + " - " + rel_entry["to"]["table"] + " - " + rel_entry["to"]["column"])
    except KeyError:
        logging.warning("Comparison schema file 'relationships' property is missing or malformed. Will continue schema comparison checks as if the schema has no relationships recorded.")
    in_tdr_not_comp = tdr_relationship_set.difference(comp_relationship_set)
    for item in in_tdr_not_comp:
        result_list.append(["Schema Comparison", item.split(" - ")[0], item.split(" - ")[1], "Relationship in TDR schema but not comparison schema (to " + item.split(" - ")[2] + "." + item.split(" - ")[3] + ")", 0, 0, 0, 0])
    in_comp_not_tdr = comp_relationship_set.difference(tdr_relationship_set)
    for item in in_comp_not_tdr:
        result_list.append(["Schema Comparison", item.split(" - ")[0], item.split(" - ")[1], "Relationship in comparison schema but not TDR schema (to " + item.split(" - ")[2] + "." + item.split(" - ")[3] + ")", 0, 0, 0, 1])
    logging.info("Relationship comparison results: \n Count relationships present in TDR schema but not comparison schema file: {0} \n Count relationships present in comparison schema file but not TDR schema: {1}".format(len(in_tdr_not_comp), len(in_comp_not_tdr)))

    # Write out and append results to dataframe
    df_results = pd.DataFrame(result_list, columns = ["metric_type", "source_table", "source_column", "metric", "n", "d", "r", "flag"])
    df = df.append(df_results)
    return df

# Main function
def main():
    # Parse arguments and configure logging
    argParser = create_arg_parser()
    parsedArgs = argParser.parse_args(sys.argv[1:])
    uuid = parsedArgs.uuid
    current_datetime = datetime.datetime.now()
    current_datetime_string = current_datetime.strftime("%Y%m%d%H%M")
    output_directory = parsedArgs.outputDirectory
    if output_directory is None:
        output_file_path = "results_{0}_{1}.tsv".format(uuid, current_datetime_string)
        log_file_path = "log_{0}_{1}.txt".format(uuid, current_datetime_string)
    else:
        if not os.path.isdir(output_directory):
            output_file_path = "results_{0}_{1}.tsv".format(uuid, current_datetime_string)
            log_file_path = "log_{0}_{1}.txt".format(uuid, current_datetime_string)
        else:
            if output_directory[-1] == "/":
                output_file_path = output_directory + "results_{0}_{1}.tsv".format(uuid, current_datetime_string)  
                log_file_path = output_directory + "log_{0}_{1}.txt".format(uuid, current_datetime_string)
            else:
                output_file_path = output_directory + "/results_{0}_{1}.tsv".format(uuid, current_datetime_string) 
                log_file_path = output_directory + "/log_{0}_{1}.txt".format(uuid, current_datetime_string)
    logging.basicConfig(format="%(asctime)s - %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p", level=logging.INFO, handlers=[logging.FileHandler(log_file_path), logging.StreamHandler(sys.stdout)])

    # Collect and validate input parameters (see definitions in create_arg_parser())
    logging.info("Starting TDR data validation...")
    logging.info("Collecting and validating input parameters...")
    if not is_valid_uuid(uuid):
        logging.error("Invalid uuid parameter passed. Please pass a valid UUID value. Exiting script.")
        sys.exit(1)
    if output_directory is None:
        logging.warning("No output directory specified in the outputDirectory parameter. Will write output file to the directory the script is run from.")  
    else:
        if not os.path.isdir(output_directory):
            logging.warning("The directory specified in the outputDirectory parameter can't be found. Will write output file to the directory the script is run from.")  
    storage_type = parsedArgs.storageType
    if storage_type is not None:
        if storage_type not in ["dataset", "snapshot"]:
            logging.warning("Invalid storageType parameter passed. Will default to using 'dataset'.")
            storage_type = "dataset"
    else:
        logging.info("No storageType parameter specified. Will default to using 'dataset'.")
        storage_type = "dataset"
    env = parsedArgs.env
    host = "https://data.terra.bio"
    if env is not None:
        if env not in ["prod", "dev"]:
            logging.warning("Invalid env parameter passed. Will default to using 'prod'.")
            env = "prod"
        elif env == "dev":
            host = "https://jade.datarepo-dev.broadinstitute.org/"
    else:
        logging.info("No env parameter specified. Will default to using 'prod'.")
        env = "prod"
    schema_file_path = parsedArgs.schemaFilePath
    run_schema_compare = False
    if schema_file_path is None:
        logging.info("No schemaFilePath parameter specified. Will skip schema comparison checks.")
    else:
        if file_exists(schema_file_path):
            try:
                with open(schema_file_path, 'r') as json_file:
                    comparison_schema = json.load(json_file)
                run_schema_compare = True
            except:
                logging.warning("Error reading in the file specified in schemaFilePath parameter. Will skip schema comparison checks. Please verify this is a properly formatted JSON file and re-run if these checks are needed.") 
        else:
            logging.warning("File not found for specified schemaFilePath parameter. Will skip schema comparison checks.")
    logging.info("Input parameters collected: \n uuid: {0} \n storage_type: {1} \n env: {2} \n schema_file_path: {3} \n run_schema_compare: {4} \n output_file_path: {5}".format(uuid, storage_type, env, schema_file_path, run_schema_compare, output_file_path))

    # Setup Google Creds
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)

    # Establish TDR client to collect schema definition
    config = data_repo_client.Configuration()
    config.host = host
    config.access_token = creds.token
    api_client = data_repo_client.ApiClient(configuration=config)
    api_client.client_side_validation = False
    
    # Retrieve the schema for the data in TDR and parse into something more useful for building queries
    logging.info("Attempting to identify the TDR object, and collect and parse its schema...")
    tdr_schema_dict, bq_project, bq_schema, skip_bq_queries = retrieve_tdr_schema(uuid, storage_type, api_client)
    table_set, array_field_set, field_list, relationship_count = process_tdr_schema(tdr_schema_dict)
    logging.info("TDR object identified and schema parsed: \n BQ project id: {0} \n BQ dataset name: {1} \n table count: {2} \n field count: {3} \n array field count: {4} \n relationships count: {5}".format(bq_project, bq_schema, len(table_set), len(field_list), len(array_field_set), relationship_count))

    # Initialize metric collect from BigQuery and create dataframe to store results 
    client = bigquery.Client()
    df = pd.DataFrame(columns = ["metric_type", "source_table", "source_column", "metric", "n", "d", "r", "flag"])

    # Run validation checks
    if run_schema_compare == True:
        df = run_schema_comparison_checks(df, tdr_schema_dict, comparison_schema) 
    if not skip_bq_queries == True:
        df, empty_table_list = run_table_profiling_checks(client, df, bq_project, bq_schema, table_set, field_list)
        df = run_column_profiling_checks(client, df, bq_project, bq_schema, field_list, array_field_set, empty_table_list)
        if storage_type == "dataset":
            df = run_orphan_file_checks(client, df, bq_project, bq_schema, field_list, array_field_set)

    # Write out results to TSV
    logging.info("Attempting to write results out to {}...".format(output_file_path))
    df_final = df.fillna(0)
    df_final.sort_values(by=["metric_type", "source_table", "source_column", "metric"], inplace=True, ignore_index=True)
    try:
        df_final.to_csv(output_file_path, index=False, sep="\t")
        logging.info("Results write-out complete.")
    except:
        new_output_file_path = "results_{0}_{1}.tsv".format(uuid, current_datetime_string)
        logging.warning("Error writing results to {0}. Attempting to write results to {1}} instead...".format(output_file_path, new_output_file_path)) 
        output_file_path = new_output_file_path
        try:
            df_final.to_csv(output_file_path, index=False, sep="\t")
            logging.info("Results write-out complete.")
        except: 
            logging.error("Write out of TDR data validaiton results failed. Exiting script.")
            sys.exit(1)
    logging.info("TDR data validation complete!")

if __name__ == "__main__":
    main()
