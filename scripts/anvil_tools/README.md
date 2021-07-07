# AnVIL Tools/Scripts

------------------------
#### This directory contains scripts/tools to automate steps in the AnVIL data delivery workspace creation process.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running any of the listed scripts:
    1. `gcloud auth login username@broadinstitute.org`
    2. `gcloud auth application-default login`
        Select your broadinstitute.org email address when window opens.

### Scripts

#### **add_user_to_workspace.py**
##### Description
    Update/add user/group to workspace ACL (as READER, WRITER, OWNER). 
    
    Input is a .tsv file with 4 columns:
        1. "workspace_name"
        2. "workspace_project"
        3. "email"
        4. "accessLevel" - (READER, WRITER, or OWNER)
##### Usage
    Locally
        `python3 /scripts/anvil_tools/add_user_to_workspace.py -t TSV_FILE`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish bash -c "cd data; python3 /scripts/anvil_tools/add_user_to_workspace.py -t /data/INPUT.tsv"`

        Note: `local_data_directory` should be the path to the folder where your desired input .tsv file is located.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)


#### **file_exists_checker.sh**
##### Description
    Check if list of files exist at denoted gs://XXXX path in GCS location.

    Input is:
        1. excel file with column of gs://XXXX file paths to validate (must end in .xlsx)
        2. number of column with gs://XXXX file paths (must be an integer value)
    Output is a .xlsx file with name:
        1. `{input_excel_filenamme}_with_exists.xlsx`
##### Usage
    Locally
        `./file_exists_checker EXCEL_FILE COLUMN_NUMBER_TO_CHECK`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish bash -c "export CLOUDSDK_PYTHON=python3.7; cd data; ./../scripts/anvil_tools/file_exists_checker.sh EXCEL_FILE COLUMN_NUMBER_TO_CHECK; unset CLOUDSDK_PYTHON"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located.
##### Flags
    1. `EXCEL_FILE`: input .xlsx file (required)
    2. `COLUMN_NUMBER_TO_CHECK`: number of column to parse from excel with gs;//XXXX paths (required)


#### **get_workspace_attributes.py**
##### Description 
    Gets the workspace's attributes from all the workspaces in a project and makes a master tsv containing Terra workspace attributes, where each row is a workspace and each column is a field in workspace attributes.
##### Usage (from the main directory):
    Locally
        `python3 /scripts/anvil_tools/get_workspace_attributes.py -wp workspace project/namespace [-v]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish bash -c "cd data; python3 /scripts/anvil_tools/get_workspace_attributes.py -wp TERRA_PROJECT [-v]"`

        Note: `local_data_directory` should be the path to the folder where your desired output .tsv file should be placed.
##### Flags
    1. `--verbose`, `-v`: set for more detailed/information to stdout (default = False)
    2. `--workspace_project`, `-wp`: workspace project/namespace (default = "anvil-datastorage")


#### **get_workspaces_list_in_project.py**
##### Description 
    Gets the list of workspaces in a project from the warehouse and saves as txt file.
##### Usage (from the main directory):
    Locally
        `python3 /scripts/anvil_tools/get_workspaces_list_in_project.py -tp terra project/namespace [-v]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish bash -c "cd data; python3 /scripts/anvil_tools/get_workspaces_list_in_project.py -tp TERRA_PROJECT [-v]"`

        Note: `local_data_directory` should be the path to the folder where your desired output .tsv file should be placed.
##### Flags
    1. `--verbose`, `-v`: set for more detailed/information to stdout (default = False)
    2. `--terra_project`, `-tp`: workspace project/namespace (default = "anvil-datastorage")


#### **post_workspace_attributes.py**
##### Description
    Post dataset attributes to workspaces.
    
    Input is a .tsv file:
        1. Template input.tsv linked [here](https://docs.google.com/spreadsheets/d/1k6fTGJL9j0p5ROsrxHIKObBwwVJFfHLpoWocZ2p8jPM/edit?usp=sharing). This document also contains definitions and examples for each column/attribute.
##### Usage
    Locally
        `python3 post_workspace_attributes.py -t TSV_FILE [-p BILLING-PROJECT]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish bash -c "cd data; python3 /scripts/anvil_tools/post_workspace_attributes.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)

#### **publish_workspaces_to_data_library.py**
##### Description
    Post workspaces listed in input tsv file to the Firecloud Data Library. Must run post_workspace_attributes.py first.
    
    Input is a .tsv file with column:
        1. `workspace_name`
    Output is a .tsv file with name:
        1. `{timestamp}_workspaces_published_status.tsv`
##### Usage
    Locally
        `python3 publish_workspaces_to_data_library.py -t TSV_FILE [-p BILLING-PROJECT]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish bash -c "cd data; python3 /scripts/anvil_tools/publish_workspaces_to_data_library.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)


#### **set_up_anvil_workspaces.py**
##### Description
    Create workspace with authorization domain and add user/groups with appropriate workspace ACLs. 
    
    Input is a .tsv file with columns:
        1. "workspace_name"
        2. "auth_domain_name"
    Output is a .tsv file with name:
        1. `{timestamp}_workspaces_setup_status.tsv`
##### Usage
    Locally
        `python3 /scripts/anvil_tools/set_up_anvil_workspace.py -t TSV_FILE [-p WORKSPACE_PROJECT]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish bash -c "cd data; python3 /scripts/anvil_tools/set_up_anvil_workspace.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located and where your output .tsv file will be placed.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)


#### **split_and_push_data_model_tsvs.py**
##### Description
    Specific collaborators or consortiums sometimes provide a single tsv file containing ONE entity type's (data table) data but for multiple workspaces - rather than the traditional single tsv per workspace. In the cases where a single tsv is provided, there are two additional columns that will be required of the user, workspace name and workspace project. These two extra columns denote which rows in the tsv need to be pushed to which workspace. This script will split the tsv into workspace specific tsv contents, create a json request, and push the table to the workspace.

    Inputs are:
        1. .tsv file - file of a single entity_type's (data table) data - must contain columns with names -
            a) "workspace_name"
            b) "workspace_project"
        2. .txt file - new line delimited .txt file with attribute/column names ONLY IF there are array type columns/attributes -
            a) ensure that the data in this column are at minimum comma separated to denote separate items in the array
        3. `--json_output` flag if a local json file with the final json request is required (see below for use and default information)
    Output is:
        1. if `--json_output` set - `{workspace_project}_{workspace_name}_batch_upsert_request.json`
        2. if `--json_output` not set - NA - console will show printouts with the success or failure of the request to push to each workspace
##### Usage
    Locally
        `python3 /scripts/anvil_tools/split_and_push_data_model_tsvs.py -t TSV_FILE [-a ARRAY_COLUMNS_FILE] [--json_output]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish bash -c "cd data; python3 /scripts/anvil_tools/split_and_push_data_model_tsvs.py -t /data/INPUT.tsv [-a /data/ARRAY_COLUMNS_FILE] [--json_output]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located and where your output .tsv file will be placed.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--array_columns`, `-a`: .txt file, new line delimited, to capture columns/attributes that are or array type (default = NO array type columns/attributes)
    3. `--json_output`: parameter to set if a local json file of the final json request is required (default = NO local json output file created)
