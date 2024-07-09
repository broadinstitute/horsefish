import argparse
import pandas as pd
from firecloud import api as fapi
from ast import literal_eval
from io import StringIO


def create_sample_tsvs(project, workspace, runID, entity_table):

    # API call to get flowcell_data table
    response = fapi.get_entities_tsv(project, workspace, entity_table, model="flexible")

    # read API response into data frame
    df = pd.read_csv(StringIO(response.text), sep="\t", index_col="entity:" + entity_table + "_id")

    # create individual lists of required data to add to new data frames for sample and sample_set tsv
    cleaned_bams_list = literal_eval(df.cleaned_reads_unaligned_bams[runID])
    batch_nums_list = [df.batch_number[runID]] * len(cleaned_bams_list)
    run_id_list = [runID] * len(cleaned_bams_list)
    sample_id_list = [bam.split("/")[-1].replace(".cleaned.bam", "") for bam in cleaned_bams_list]
    participant_id_list = [sample_id.split(".")[0] for sample_id in sample_id_list]

    # create sample.tsv data frame (entity:sample_set_id)
    df_sample_table = pd.DataFrame({"entity:sample_id": sample_id_list,
                                    "batch": batch_nums_list,
                                    "run_id": run_id_list,
                                    "cleaned_bam": cleaned_bams_list})
    # create sample_set.tsv data frame (membership:sample_set_id)
    df_sample_set_table = pd.DataFrame({"membership:sample_set_id" : participant_id_list,
                                        "sample": sample_id_list})

    # file name prefixes
    sample_name = runID + "_sample_table.tsv"
    set_name = runID + "_sample_set_table.tsv"

    # write data frames to .tsv files
    df_sample_table.to_csv(sample_name, sep="\t", index=False)
    print(f"Created file {sample_name}.")

    df_sample_set_table.to_csv(set_name, sep="\t", index=False)
    print(f"Created file {set_name}.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--workspace_name', required=True, help='name of workspace in which to make changes')
    parser.add_argument('--workspace_project', required=True, help='billing project (namespace) of workspace in which to make changes')
    parser.add_argument('--run_id', required=True, help='specific runID')
    parser.add_argument('--entity_table', default='flowcell_data', help='name of table with run data and cleaned bam list')

    args = parser.parse_args()

    create_sample_tsvs(args.workspace_project, args.workspace_name, args.run_id, args.entity_table)