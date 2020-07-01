import argparse
import pandas as pd

def join_tsvs(tsvs):
    'Create a combined metadata file from individual to use as in put for Nexststrain augur workflow.'
    # read each file in input list as a dataframe and add to list
    df_list = []
    for file in tsvs:
        df = pd.read_csv(file, sep="\t", dtype=str)
        df_list.append(df)

    # concatenate dataframes with dtype as string = OK
    # input test files: tab-1.txt tab-2.txt
    df_concat = pd.concat(df_list, axis=0, ignore_index=True, sort=False).fillna('NA')

    # test pandas merge instead of concat
    # df_merged = pd.merge(tab1, tab2, how='outer', on=['strain'], left_index=True, right_index=True)

    # write concatenated dataframe to file
    pd.DataFrame.to_csv(df_concat, 'combined_metadata.tsv', sep='\t', index=False)
    # write merged dataframe to file
    # pd.DataFrame.to_csv(df_merged, 'merged_combined_metadata.tsv', sep='\t', index=False)

    #TODO: determine unique list of column names
    #TODO: optional NA fill-in, how to handle merge with more than 2 dataframes.
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--tsv_files', nargs='+', help='arrray of tsv files to combine', required=True)

    args = parser.parse_args()
    join_tsvs(args.tsv_files)
