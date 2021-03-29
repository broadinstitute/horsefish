# Van Allen Lab Tools/Scripts

------------------------
#### This directory contains scripts/tools for Van Allen Lab processes.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running any of the listed scripts:
    1. `gcloud auth login --update-adc`
        Select your broadinstitute.org email address when window opens.

### Scripts

#### **set_up_vanallen_workspaces.py**
##### Description
    Create workspace with authorization domain and add user/groups with appropriate workspace ACLs. 
    
    Input is a .tsv file with columns:
        1. "workspace_name"
    Output is a .tsv file with name:
        1. `{timestamp}_workspaces_setup_status.tsv`
##### Usage
    Locally
        `python3 /scripts/anvil_tools/set_up_vanallen_workspaces.py -t TSV_FILE [-p WORKSPACE_PROJECT]
    Docker
        `docker run --rm -it -v "$HOME"/.config:/.config -v "$HOME"/local_data_directory/:/data broadinstitute/horsefish:anvil_tools bash -c "cd data; python3 /scripts/anvil_tools/set_up_vanallen_workspaces.py -t /data/INPUT.tsv [-p WORKSPACE_PROJECT]"`

        Note: local_data_directory should be the path to the folder where your input .tsv file is located and where your output .tsv file will be placed.
##### Flags
    1. `--tsv`, `-t`: input .tsv file (required)
    2. `--project`, `-p`: workspace project/namespace for listed workspaces in tsv (default = vanallen-firecloud-nih)