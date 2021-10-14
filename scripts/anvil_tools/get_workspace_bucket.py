"""Get Google Storage bucket ID for a given workspace and workspace namespace.
Usage:
    > python3 get_workspace_bucket.py -t TSV_FILE """

import argparse
import json
import pandas as pd
import requests

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Get workspace bucket.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with workspace name and workspace project columns.')

    args = parser.parse_args()

    # call to create and set up external data delivery workspaces
    get_workspace_bucket(args.tsv)