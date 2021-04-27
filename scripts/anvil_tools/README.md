# AnVIL Tools/Scripts

------------------------
#### This directory contains scripts/tools to automate steps in the AnVIL data delivery workspace creation process.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running any of the listed scripts:
    1. `gcloud auth login --update-adc`
        Select your broadinstitute.org email address when window opens.

### Scripts

#### **add_user_to_authorization_domain.py**
##### Description
    Update/add user/group to authorization domain (as ADMIN, MEMBER). To run this script, you must be an ADMIN on the authorization domain(s). 
    
    Input is a .tsv file with 3 columns:
        1. "auth_domain_name" - no `@firecloud.org`
        2. "email"
        3. "accessLevel" - (MEMBER or ADMIN)
##### Usage
    Locally
        `python3 /scripts/anvil_tools/add_user_to_authorization_domain.py -t TSV_FILE [-p WORKSPACE_PROJECT]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:anvil_tools bash -c "cd data; python3 /scripts/anvil_tools/add_user_to_authorization_domain.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: `local_data_directory` should be the path to the folder where your desired input .tsv file is located.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)


#### **add_user_to_workspace.py**
##### Description
    Update/add user/group to workspace ACL (as READER, WRITER, OWNER). 
    
    Input is a .tsv file with 3 columns:
        1. "workspace_name"
        2. "email"
        3. "accessLevel" - (READER, WRITER, or OWNER)
##### Usage
    Locally
        `python3 /scripts/anvil_tools/add_user_to_workspace.py -t TSV_FILE [-p WORKSPACE_PROJECT]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:anvil_tools bash -c "cd data; python3 /scripts/anvil_tools/add_user_to_workspace.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: `local_data_directory` should be the path to the folder where your desired input .tsv file is located.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)


#### **get_workspace_attributes.py**
##### Description 
    Gets the workspace's attributes from all the workspaces in a project and makes a master tsv containing Terra workspace attributes, where each row is a workspace and each column is a field in workspace attributes.
##### Usage (from the main directory):
    Locally
        `python3 /scripts/anvil_tools/get_workspace_attributes.py -wp workspace project/namespace [-v]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:anvil_tools bash -c "cd data; python3 /scripts/anvil_tools/get_workspace_attributes.py -wp TERRA_PROJECT [-v]"`

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
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:anvil_tools bash -c "cd data; python3 /scripts/anvil_tools/get_workspaces_list_in_project.py -tp TERRA_PROJECT [-v]"`

        Note: `local_data_directory` should be the path to the folder where your desired output .tsv file should be placed.
##### Flags
    1. `--verbose`, `-v`: set for more detailed/information to stdout (default = False)
    2. `--terra_project`, `-tp`: workspace project/namespace (default = "anvil-datastorage")


#### **set_up_anvil_workspaces.py**
##### Description
    Create workspace with authorization domain and add user/groups with appropriate workspace ACLs. 
    
    Input is a .tsv file with columns:
        1. "workspace_name"
        2. "auth_domain_name"
    Output is a .tsv file with name:
        1. `{timestamp}_workspaces_full_setup_status.tsv`
##### Usage
    Locally
        `python3 /scripts/anvil_tools/set_up_anvil_workspace.py -t TSV_FILE [-p WORKSPACE_PROJECT]
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:anvil_tools bash -c "cd data; python3 /scripts/anvil_tools/set_up_anvil_workspace.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located and where your output .tsv file will be placed.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)


#### **publish_dataset_attributes_to_workspace.py**
##### Description
    Publish dataset attributes to workspaces. workspaces listed in input tsv file to the Firecloud Data Library. Must run first before publish_workspaces_to_data_library.py.
    
    Input is a .tsv file:
        1. Template input.tsv linked [here](https://docs.google.com/spreadsheets/d/1k6fTGJL9j0p5ROsrxHIKObBwwVJFfHLpoWocZ2p8jPM/edit?usp=sharing). This document also contains definitions and examples for each column/attribute.
    Output is a .tsv file with name:
        1. `{timestamp}_workspaces_dataset_attributes_published_status.tsv`
##### Usage
    Locally
        `python3 publish_dataset_attributes_to_workspace.py -t TSV_FILE [-p BILLING-PROJECT]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:anvil_tools bash -c "cd data; python3 /scripts/anvil_tools/publish_dataset_attributes_to_workspace.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)


#### **publish_workspaces_to_data_library.py**
##### Description
    Post workspaces listed in input tsv file to the Firecloud Data Library. Must run publish_dataset_attributes_to_workspace.py first.
    
    Input is a .tsv file with column:
        1. `workspace_name`
    Output is a .tsv file with name:
        1. `{timestamp}_workspaces_published_to_data_library_status.tsv`
##### Usage
    Locally
        `python3 publish_workspaces_to_data_library.py -t TSV_FILE [-p BILLING-PROJECT]`
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:anvil_tools bash -c "cd data; python3 /scripts/anvil_tools/publish_workspaces_to_data_library.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = anvil_datastorage)