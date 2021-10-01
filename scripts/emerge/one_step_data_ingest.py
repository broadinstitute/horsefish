import argparse
import json
import pandas as pd


def create_recoded_json(row_json):
    """Update dictionary with TDR's dataset relative paths for keys with gs:// paths."""

    all_row_jsons = []
    for key in row_json.keys():
        value = str(row_json[key])  # convert to string to be able to check if gs:// path

        # if value exists and has 'gs://'
        if value is not None and "gs://" in value:
            relative_tdr_path = value.replace("gs://","/")  # create TDR relative path

            # # TODO: add in description = id_col + col_name
            row_json[key] = {"sourcePath":value,
                             "targetPath":relative_tdr_path,
                             "mimeType":"text/plain"
                            }
    return row_json


def create_newline_delimited_json(input_tsv):
    """Create newline delimited json file from input tsv."""

    tsv_df = pd.read_csv(input_tsv, sep="\t")

    basename = input_tsv.split(".tsv")[0]
    output_filename = f"{basename}_recoded_newline_delimited.json"

    all_rows = []
    for row in tsv_df.iterrows():
        row_dict = json.loads(row[1].to_json())  # create dictionary for one sample/row
        # recode the row json with gs:// path ingest format if applicable
        recoded_row_dict = create_recoded_json(row_dict)
        # add recoded row's dictionary to list
        all_rows.append(recoded_row_dict)

    # write list of dictionaries to file
    with open(output_filename, 'w') as final_newline_json:
        for r in all_rows:
            json.dump(r, final_newline_json)
            final_newline_json.write('\n')

    return output_filename
    print(f"Recoded newline delimited json created: {output_filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a newline delimited json for bulk ingesting data into a TDR dataset.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv (.tsv) file with file ingest information per file.')
    args = parser.parse_args()

    create_newline_delimited_json(args.tsv)