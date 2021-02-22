# AnVIL Tools/Scripts

------------------------
#### This directory contains scripts/tools to automate steps in the AnVIL data delivery workspace creation process.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running the scripts:
    1. `gcloud auth login username@broadinstitute.org`
    2. `gcloud auth application-default login`

### Scripts

#### get_workspace_attributes.py
##### Description 
        >Gets the workspace's attributes from all the workspaces in a project and makes a master csv containing Terra workspace attributes, where each row is a workspace and each column is a field in workspace attributes.
##### Usage (from the main directory):
        >Locally
            `python3 /scripts/anvil_tools/get_workspace_attributes.py -v -wp [workspace project/namespace]`
##### Flags
    >`--verbose`, `-v`: set for more detailed/information to stdout (default = False)
    >`--workspace_project`, `-wp`: workspace project/namespace (default = "anvil-datastorage")


#### get_workspaces_list_in_project.py
##### Description 
        >Gets the list of workspaces in a project from the warehouse and saves as txt file.
##### Usage (from the main directory):
        >Locally
            `python3 /scripts/=anvil_tools/get_workspaces_list_in_project.py -v -tp [terra's project/namespace]`
##### Flags
    >`--verbose`, `-v`: set for more detailed/information to stdout (default = False)
    >`--terra_project`, `-tp`: workspace project/namespace (default = "anvil-datastorage")

#### add_user_to_workspace.py
##### Description
        >Update/add user/group to workspace ACL (as READER, WRITER, OWNER). Input is a .tsv file with 3 columns:
            1. "workspace_name"
            2. "email"
            3. "accessLevel"
##### Usage
        >Locally
            `python3 /scripts/anvil_tools/add_user_to_workspace.py -t TSV_FILE [-p WORKSPACE_PROJECT]`
        >Docker
            `docker run --rm -v "$HOME"/.config:/.config -v ~/input_data_directory/:/data broadinstitute/horsefish:anvil_tools  python3 /scripts/anvil_tools/add_user_to_workspace.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]`

            Note: input_data_directory should be the path to the folder where your input .tsv file is located.
##### Flags
    >`--tsv`, `-t`: input .tsv file (required)
    >`--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)

#### set_up_anvil_workspace.py
##### Description
        >Create workspace with authorization domain and add user/groups with appropriate workspace ACLs. Input is a .tsv file with columns:
            1. "workspace_name"
            2. "auth_domain_name"
##### Usage
        >Locally
            `python3 /scripts/anvil_tools/set_up_anvil_workspace.py -t TSV_FILE [-p WORKSPACE_PROJECT]
        >Docker
            `docker run --rm -v "$HOME"/.config:/.config -v ~/input_data_directory/:/data broadinstitute/horsefish:anvil_tools  python3 /scripts/anvil_tools/set_up_anvil_workspace.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]`

            Note: input_data_directory should be the path to the folder where your input .tsv file is located.
##### Flags
    >`--tsv`, `-t`: input .tsv file (required)
    >`--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)

