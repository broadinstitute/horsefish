# Create a tsv list of workspaces
python3 scripts/van_allen_tools/create_tsv.py -w "ms_test"

# Create workspaces with us-central1 buckets
python3 scripts/van_allen_tools/set_up_vanallen_workspaces.py -t "output.tsv" -n "broad-firecloud-dsde"

# Copy over data table and workflows
python3 scripts/van_allen_tools/copy_workspace_data.py -fn "broad-firecloud-dsde" -fw "tutorial-synthetic_data_set_1_test" -tn "broad-firecloud-dsde" -tw "ms_test"


