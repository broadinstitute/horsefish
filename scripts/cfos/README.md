# cFos Tools/Scripts

------------------------
#### This directory contains scripts/tools to automate steps in the cFos metadata delivery to workspaces in the form of entity data tables.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running any of the listed scripts:
    1. `gcloud auth login username@broadinstitute.org`
    2. `gcloud auth application-default login`
        Select your broadinstitute.org email address when window opens.

### Scripts

#### **make_dataset_data_tables.py**
##### Description
    WIP: Creates Terra entity model data tables in a Terra workspace given an input file containing metadata for a dataset.
    
    Inputs required are:
        1. .xlsx file in the template format for the specific dataset
        2. .json file containing schema of datasets, their tables, and each table's column organization
        3. dataset name

    Inputs optional are:
        1. validate flag - only validates the input excel file to check each columns values against defined validation rules
        2. upload flag - pushes generated load table tsv files to Terra workspace
            - requires workspace name and workspace project arguments

##### Usage
    1. only validate input excel file
        `python3 /scripts/cfos/make_dataset_data_tables.py -x EXCEL_FILE -d DATASET_NAME -j SCHEMA_JSON -v`
    
    2. only validate and create load tsv files locally but no upload to Terra workspace
        `python3 /scripts/cfos/make_dataset_data_tables.py -x EXCEL_FILE -d DATASET_NAME -j SCHEMA_JSON`
    
    3. validate input file, create load tsv files locally, and upload to Terra workspace
        `python3 /scripts/cfos/make_dataset_data_tables.py -x EXCEL_FILE -d DATASET_NAME -j SCHEMA_JSON -u -p WORKSPACE_PROJECT -w WORKSPACE_NAME`

##### Flags
    1. `--excel`, `-x`: input .xlsx file (required)
    2. `--dataset_name`, `-d`: name of dataset (required)
    3. `--schema`, `-j`: schema file that contains definitions of dataset tables and columns (required)
    4. `--validate`, `-v`: flag to run only validation on input .xlsx file (optional)
    5. `--upload`, `-u`: flag to upload data tables to workspace (optional)
    6. `--project`, `-p`: terra workspace project/namespace (optional - unless using --upload flag)
    7. `--workspace`, `-w`: terra workspace name (optional - unless using --upload flag)


### Resources

#### **validate.py**
##### Description
    WIP: Non-executable python file that contains the validation rules for each column in the input excel file. The validation rules are applied using pandas_schema.

#### **dataset_tables_schema.json**
##### Description
    WIP: File containing a dataset (by name) and the dataset's tables, as well as each table's organization structure - which columns are organized into which tables for a given dataset.

    Generated in a json format and can be expanded either by addition additional dataset definisions or appending tables and/or columns to existing definition.