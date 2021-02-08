"""Script to create data tables via API using input tsv file."""
import argparse
from firecloud import api as fapi
import json
import pandas as pd


def api_upload_entities(tsv, workspace, project):
    """Call API and create/update data tables."""

    response = fapi.upload_entities_tsv(project, workspace, tsv, model="flexible")
    if response.status_code != 200:
        print(f'ERROR UPLOADING: See full error message: {response.text}')
    else:
        print(f"Upload complete. Check your workspace for new {tsv.replace('.tsv', '')} table!")


def create_reads_table(bam_paths_file, metadata_json, workspace, project):
    """Create tsv and load to create "reads" data table in Terra UI."""

    # get json into dictionary
    metadata_dict = json.load(open(metadata_json))

    with open(bam_paths_file, 'r') as f:
        bam_paths = (f.read()).split(",")

    # bam = full gs path
    for bam_path in bam_paths:
        bam_file_id = (bam_path.split("/")[-1]).split(".cleaned.bam")[0]

        # if bam file is not associated with a key in metadata_by_filename_json
        if bam_file_id not in metadata_dict:
            print(f"The cleaned bam {bam_file_id} located at {bam_path} is not associated with metadata in json input.")
            return

        # if exists, add bam path to dictionary
        metadata_dict[bam_file_id]['cleaned_bam'] = bam_path

    # convert dictionary to dataframe
    load_tsv_df = pd.DataFrame.from_dict(metadata_dict, orient='index')
    # update header column name to required Terra format
    load_tsv_df.index.name = 'entity:reads_id'

    # create tsv file
    reads_tsv = "reads.tsv"
    load_tsv_df.to_csv(reads_tsv, sep="\t")

    # call fiss api to create table
    api_upload_entities(reads_tsv, workspace, project)


def create_assemblies_table(tsv, workspace, project):
    """Create tsv and load to create "assemblies" data table in Terra UI.."""

    raw_tsv_df = pd.read_csv(tsv, sep="\t")
    # reorder tsv: sample_sanitized becomes assemblies_id column and sample column stays the same
    load_tsv_df = raw_tsv_df[['sample_sanitized'] + [col for col in raw_tsv_df.columns if col != 'sample_sanitized']]
    # update header column name to required Terra format
    load_tsv_df = load_tsv_df.rename(columns={'sample_sanitized': 'entity:assemblies_id'})

    # create tsv file
    assemblies_tsv = "assemblies.tsv"
    load_tsv_df.to_csv(assemblies_tsv, sep="\t", index=False)

    # call fiss api to create table
    api_upload_entities(assemblies_tsv, workspace, project)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create data tables from input.tsv via API.")

    parser.add_argument('-t', '--tsv', type=str, required=True, help='path to tsv file.')
    parser.add_argument('-p', '--project', type=str, required=True, help='name of terra project associated with workspace.')
    parser.add_argument('-w', '--workspace', required=True, type=str, help='name of workspace.')
    parser.add_argument('-b', '--cleaned_bams', type=str, help='file of cleaned bams (post demux).')
    parser.add_argument('-j', '--meta_json', type=str, help='metadata by filename json.')

    args = parser.parse_args()

    # create assemblies table in Terra UI
    create_assemblies_table(args.tsv, args.workspace, args.project)

    # create reads table in Terra UI
    create_reads_table(args.cleaned_bams, args.meta_json, args.workspace, args.project)
