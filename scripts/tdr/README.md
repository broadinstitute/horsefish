# Scripts and functions to interact with the Terra Data Repo (TDR).
For more information about TDR, check out [the documentation](https://support.terra.bio/hc/en-us/articles/4407244408347-Terra-Data-Repository-Overview).


## Scripts and tools overview

### Python scripts:

*export_snapshot_to_data_model.py -s <snapshot_id> -p <terra_project> -w <terra_workspace>* takes a TDR snapshot (identified by snapshot_id) and copies it into a Terra workspace's data model as an entity table. Any files included in the snapshot remain in the TDR bucket, and references to those files are resolved to gs:// paths. The parent snapshot in TDR is also automatically shared (read access) with all readers, writers, and owners on the Terra workspace.

*soft_delete_rows.py -r <row_id_1> <row_id_2> [optional: -t <table>] -d <dataset_id> -p <terra_project> -w <terra_workspace>* deletes rows (identified by datarepo_row_id) from a TDR dataset. The Terra variables define the workspace bucket to use as a staging bucket for the job control file. The table that contains the rows to be deleted defaults to 'sample' but can be specified with -t. 

*update_tdr_dataset_with_workflow_outputs.py -c <path_to_config> -d <dataset_id> -s <submission_id>  -p <terra_project> -w <terra_workspace>* gathers outputs from a Terra submission (defined by submission_id), configures them for a TDR dataset using a config file that includes workflow name, a mapping of outputs to TDR fields, and (optionally) a metrics file that can be exploded into TDR fields. Because this is an "update" script, we gather existing row data for each sample from TDR, update it with the workflow outputs, ingest the new updated row, and soft delete the old row.

*create_schema_json.py -c <path_to_config>* generates the json required to create a dataset, using structured info in a config file. The config file is json and is structured as follows:
```
{
    "project_name": "PROJECT_NAME, e.g. myproject",
    "dataset_name": "DATASET_NAME, e.g. myproject_v1",
    "description": "DESCRIPTION OF TDR DATASET",
    "tables_with_pks": {
        "TABLE_1_NAME": "TABLE_1_PRIMARY_KEY",
        "TABLE_2_NAME": "TABLE_2_PRIMARY_KEY"
    },
    "assets": [
        "asset_name_1",
        "asset_name_2"
    ],
    "region": "REGION",
    "billing_profile": "BILLING PROFILE UUID",
    "enableSecureMonitoring": BOOLEAN
}
```
Any `table` listed in the config must correspond with a tsv file that defines that table's fields. The path to the tsv should be `{project_name}/schema_configs/{table_name}_table_schema.csv`. The table schema csv should have the following headers/format:
```
name,datatype,array_of
FIELD_NAME,DATA_TYPE,TRUE
```
Note that the array_of column can be blank, which is interpreted as array_of=False.

NOTE: Relationships between tables are supported in TDR but are not yet supported in this script.


### Python utilies:

*tdr_utils.py* contains common functions specific to TDR that may be reused.
*terra_utils.py* contains common functions specific to Terra that may be reused.
*utils.py* contains common functions that may be reused.


### Shell scripts:

*get_job_result.sh <job_id>* bash wrapper around API calls to get job results
*get_job_status.sh <job_id>* bash wrapper around API calls to get job status

*create_dataset.sh <schema.json>* bash wrapper around API call to create a dataset with an existing json payload file defining the dataset's schema and other options.



## How to use these scripts
You may have use for subsets of the scripts here, depending on your use case. Some common use cases are listed here with more detailed instructions.

### Creating new datasets
To create a new dataset, you'll need to define a schema. The scripts here take as input a configuration file that in turn points to a single CSV file per table in your dataset. The requirements are described below; see also an example setup in the `quickstart` directory.

