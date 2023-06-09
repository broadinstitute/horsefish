import subprocess
import argparse
import pandas as pd
"""
Script to sync between a source google bucket (aka staging) and
a destination google bucket (aka production). 
Takes in a tab-delimited file with a header and two columns:
Destination directory and Source directory.

It runs gsutil rsync for each pair of folders, copying all
changed files from destination to source.
"""


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file',
                        help="Path to tsv file containing list of \
                        source (column 1) and destination (column 2)\
                        gsURIs to sync between")
    return parser.parse_args()


def get_paths_df(input_file):
    """Creates dataframe of source and destination path pairs."""
    return pd.read_csv(input_file, sep="\t")


def main(input_file):
    paths_df = get_paths_df(input_file)
    for source_directory, destination_directory in paths_df.itertuples(
                index=False, name="Paths"):
        subprocess.call(
            # ["rsync", "-rv", source_directory, destination_directory])
            ["gsutil", "-m", "rsync", "-r",
                source_directory, destination_directory]
        )


if __name__ == "__main__":
    args = parse_args()
    main(args.input_file)
