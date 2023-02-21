# Workspace cleanup scripts

The scripts in this directory can be used to delete Terra workspaces in bulk. Proceed with caution!

`get_workspace_list.py` gathers a list of workspaces in a given project, along with information about storage cost and owners. The output of that script is a csv containing information about the workspaces that should help the user decide which they want to delete. The user should manually curate/whittle it down to only the workspaces they want to delete (and only the `project` and `workspace` columns) to pass to the delete_workspaces script.

`delete_workspaces.py` deletes a list of workspaces.

These scripts use the Terra APIs, so they can only delete workspaces that the running user has access to. If your goal is to view as many workspaces as possible in the project, it's best to make sure the person running the script is a project owner on the Terra project. Note that any workspace covered by an Authorization Domain (AD) will not be able to be deleted (or have cost information returned) by these scripts unless the person running the script is in the workspace's AD.

## Docker image & run command
To run the scripts in this directory, you can use the Docker image `broadinstitute/horsefish:v2.0`. \
This image is built from the Dockerfile in the top level `horsefish` directory in this repo. 

Authorize (`gcloud auth login`) as the user you use for Terra that has access to the Terra workspaces you want to delete or list - see above for more details.

Run the scripts using the following command **from the scripts/workspace_cleanup directory**: \
`docker run --rm -v "$HOME"/.config:/.config -v $(pwd):/workspace_cleanup broadinstitute/horsefish:v2.0 python3 workspace_cleanup/<script.py> <args>`
where `<script.py>` is the script you want to run.

## Running `get_workspace_list.py`

This script takes the following required parameter:
- `--project` or `-p` : defines the Terra project/namespace for which to return workspaces

It takes the following optional parameters:
- `--get_cost` or `-c` : flag to include estimated storage cost for the workspace bucket
- `--verbose` or `-v` : flag to print out progress text

For example, if you want to generate a list of all workspaces in your project `my-project`, you would run:

`python3 get_workspace_list.py -p my-project -c -v`

The output of the script is a file with the following headers:
`workspace,created by,[storage cost estimate,]date created,link`

We recommend you copy that into a Google sheet and manually whittle the list down to the workspaces you wish to delete, to use as input to the delete workspace script.


## Running `delete_workspaces.py`

This script takes an input csv file with the headers `workspace,project` and attempts to delete all workspaces in the list. The only input parameter it takes is `--csvpath` to define the csv.

For example, to delete a list of workspaces enumerated in a local file `my-workspaces-to-delete.csv`, you would run:

`python3 delete_workspaces.py --csvpath my-workspaces-to-delete.csv`

For ease when running via the Docker container, put the csv file you want to use in the `workspace_cleanup` directory, 
and use the path `workspace_cleanup/<csvfile.csv>` as the value for `--csvpath`.

The script will print out its progress and the final number of workspaces successfully deleted.

## Updating the Scripts or Docker image in this directory
If you want to make changes to the contents of this directory*, first create a development branch. Then, make your changes and update the Docker image to reflect those changes. To do so, **edit line 4 of the `update_docker_image.sh` script in the top level directory to update the Docker image version**. Then run the script. \
_*Do not add the csv of workspaces to delete to this repo. It should be kept locally or deleted after running._
