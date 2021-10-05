import argparse
import json
import pandas as pd

REPEATED_FILES = set()
UNIQUE_FILES = set()

def create_recoded_json(row_json):
    """Update dictionary with TDR's dataset relative paths for keys with gs:// paths."""

    recoded_row_json = dict(row_json)
    # print(f"START LOOP row json ---- {row_json}")
    # print(f"START LOOP recoded_row_json ---- {recoded_row_json}")

    for key in row_json.keys():
        value = str(row_json[key])  # convert to string to be able to check if gs:// path

        # if value exists and has 'gs://' and is not already in set of gs paths- recode path with expanded request
        if value is not None and value.startswith("gs://") and value not in REPEATED_FILES:
            relative_tdr_path = value.replace("gs://","/")  # create TDR relative path

            # TODO: add in description = id_col + col_name
            recoded_row_json[key] = {"sourcePath":value,
                            "targetPath":relative_tdr_path,
                            "mimeType":"text/plain"
                            }
            # add updated file to set so its not recoded in any other row/column
            REPEATED_FILES.add(value)

    print(f"row json ---- {row_json}")
    print(f"recoded_row_json ---- {recoded_row_json}")
    print(f"REPEATED_FILES ---- {REPEATED_FILES}")
    return recoded_row_json


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

    print(f"Recoded newline delimited json created: {output_filename}")
    return output_filename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a newline delimited json for bulk ingesting data into a TDR dataset.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv (.tsv) file with file ingest information per file.')
    args = parser.parse_args()

    create_newline_delimited_json(args.tsv)