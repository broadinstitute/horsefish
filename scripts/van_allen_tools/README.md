# Van Allen Lab Tools/Scripts

------------------------
#### This directory contains scripts/tools for Van Allen Lab processes.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running any of the listed scripts:
    1. `gcloud auth login --update-adc`
        Select your broadinstitute.org email address when window opens.

### Scripts

#### **query_bucket_object_inventory.py**
##### Description
    Query BQ table for a single bucket's object inventory and export results to GCS location as a .csv file. Table to query needs to be created ahead of time via gcs-inventory-loader tool.
    
    Input is:
        1. "bucket_id" (String)
    Output is a .csv file with name:
        1. `{bucket_id}_source_details.csv` in `gs://bigquery-billing-exports/bucket_inventory_files/`
##### Usage
    Locally
        `python3 /scripts/van_allen_tools/query_bucket_object_inventory.py -b bucket_name (with or without 'gs://' is accepted)

##### Flags
    1. `--source_bucket_name`, `-b`: bucket_id - fc-****** (required)
    2. `--output_file_bucket`, `-o`: bucket name to export BQ tables as csv files (default = gs://bigquery-billing-exports)
    3. `--gcp_project`, `-p`: google project where Big Query datasets and tables are stored (default = vanallen-gcp-nih)
    4. `--bq_dataset`, `-d`: name of destination dataset in BigQuery project to write query results (default =      gcs_inventory_loader)
    5. `--bq_inventory_table`, `-t`: table name of object inventory (loaded with gcs-inventory-loader) (default = object_metadata)


#### **migrate_van_allen_workspaces.py**
##### Description
    Create workspace with authorization domain and add user/groups with appropriate workspace ACLs. 
    
    Input is a .tsv file with columns:
        1. "source_workspace_name"
        2. "source_workspace_namespace"
        3. "destination_workspace_name"
        4. "destination_workspace_namespace"
    Output is a .tsv file with name:
        1. `{timestamp}_workspaces_published_status.tsv`
        2. `{timestamp}_workspaces_published_terra_datamodel.tsv`
##### Usage
    Locally
        `python3 /scripts/van_allen_tools/set_up_vanallen_workspaces.py -t TSV_FILE
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:latest bash -c "cd data; python3 /scripts/van_allen_tools/set_up_vanallen_workspaces.py -t /data/INPUT.tsv"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located and where your output .tsv file will be placed.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)


#### **set_up_vanallen_workspaces.py**
##### Description
    Create workspace with authorization domain and add user/groups with appropriate workspace ACLs. 
    
    Input is a .tsv file with columns:
        1. "workspace_name"
    Output is a .tsv file with name:
        1. `{timestamp}_workspaces_setup_status.tsv`
##### Usage
    Locally
        `python3 /scripts/van_allen_tools/set_up_vanallen_workspaces.py -t TSV_FILE [-n WORKSPACE_PROJECT]
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:latest bash -c "cd data; python3 /scripts/van_allen_tools/set_up_vanallen_workspaces.py -t /data/INPUT.tsv [-n NAMESPACE]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located and where your output .tsv file will be placed.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--workspace_namespace`, `-n`: workspace project/namespace for listed workspaces in tsv (default = vanallen-firecloud-nih)