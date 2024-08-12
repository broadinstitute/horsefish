import argparse
import pandas as pd


def join_tsvs(tsvs):
    'Create a combined metadata file from individual metadata files.'
    # read each file in input list as a dataframe and add to list
    df_list = []
    for file in tsvs:
        df = pd.read_csv(file, sep="\t", dtype=str)
        df_list.append(df)

    # concatenate dataframes
    df_concat = pd.concat(df_list, axis=0, ignore_index=True, sort=False).fillna('NA')

    # write concatenated dataframe to file
    pd.DataFrame.to_csv(df_concat, 'combined_metadata.tsv', sep='\t', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--tsv_files', nargs='+', help='arrray of tsv files to combine', required=True)

    args = parser.parse_args()
    join_tsvs(args.tsv_files)