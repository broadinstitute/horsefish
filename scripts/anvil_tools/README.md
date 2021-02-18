# AnVIL Tools/Scripts

------------------------
#### This directory contains scripts/tools to automate steps in the AnVIL data delivery workspace creation process.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running the scripts:
    1. `gcloud auth login username@broadinstitute.org`
    2. `gcloud auth application-default login`

## get_workspace_attributes.py
#### Description 
>Gets the workspace's attributes from all the workspaces in a project and makes a master csv containing Terra workspace attributes, where each row is a workspace and each column is a field in workspace attributes.
#### Setup
>1. Run `gcloud auth login` with broadinstitute.org email
#### Flags
>`--verbose` or `-v` for Verbose option to have more information outputed (Default is False)
>`--workspace_project` or `-wp` to input the Workspace Project/Namespace (Default is "anvil-datastorage")
#### Usage (from the main directory):
python3 projects/get_workspace_attributes/get_workspace_attributes.py -v -wp <workspace's project/namespace input>


## get_workspaces_list_in_project.py
#### Description 
>Gets the list of workspaces in a project from the warehouse and saves as txt file.
#### Setup
>1. Run `gcloud auth login` with firecloud.org email
>2. Run `gcloud auth application-default login` with firecloud.org email
>3. Run `gcloud auth application-default set-quota-project broad-dsde-prod-analytics-dev`
#### Flags
>`--verbose` or `-v` for Verbose option to have more information outputed (Default is False)
>`--terra_project` or `-tp` to input the Terra Project/Namespace (Default is "anvil-datastorage")
#### Usage (from the main directory):
python3 projects/get_workspace_attributes/get_workspaces_list_in_project.py -v -tp <terra's project/namespace input>
