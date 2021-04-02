"""Create a tsv of Van Allen Lab workspaces.

Usage:
    >python3 scripts/van_allen_tools/create_tsv.py -w WORKSPACE_NAME1 WORKSPACE_NAME2 WORKSPACE_NAME3
"""
import csv
import argparse


def create_workspaces_tsv(workspaces):
    """Create a tsv of Van Allen Lab workspaces."""
    with open('output.tsv', 'wt') as out_file:
        tsv_writer = csv.writer(out_file, delimiter='\t')
        tsv_writer.writerow(['workspace_name'])
        for workspace_name in workspaces:
            tsv_writer.writerow([workspace_name])


# Main Function
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create a tsv of Van Allen Lab workspaces.')
    parser.add_argument('-w', '--workspaces', required=True, nargs='+', help='list of Van Allen Lab workspaces: WORKSPACE_NAME1 WORKSPACE_NAME2')

    args = parser.parse_args()

    # call create tsv
    create_workspaces_tsv(args.workspaces)
