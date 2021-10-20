import argparse
import json
import pandas as pd


def create_recoded_json(row_json):
    """Update dictionary with TDR's dataset relative paths for keys with gs:// paths."""

    recoded_row_json = dict(row_json)  # update copy instead of original

    for key in row_json.keys():  # for column name in row
        value = row_json[key]    # get value
        # if value exists - not an empty cell/entry
        if value is not None:
            # and starts with 'gs://' --> recode path with expanded request
            if value.startswith("gs://"):
                relative_tdr_path = value.replace("gs://","/")  # create TDR relative path
                # TODO: add in description = id_col + col_name
                recoded_row_json[key] = {"sourcePath":value,
                                "targetPath":relative_tdr_path,
                                "mimeType":"text/plain"
                                }

            recoded_row_json_list = []  # instantiate empty list to store recoded values
            # if value is of type array
            if value.startswith("[") and value.endswith("]"):
                value_list = json.loads(value)  # convert <str> to <liist>
                # and list values start with 'gs://'
                paths = [item.startswith('gs://') for item in list(value_list)]
                if any(paths):
                    # for each item in the array, create the json request
                    for item in value_list:
                        relative_tdr_path = item.replace("gs://","/")  # create TDR relative path
                        x = {"sourcePath":item,
                            "targetPath":relative_tdr_path,
                            "mimeType":"text/plain"
                            }
                        recoded_row_json_list.append(x)  # add json request to list
                recoded_row_json[key] = recoded_row_json_list  # add list of json requests to larger json request

    return recoded_row_json


def create_newline_delimited_json(input_tsv):
    """Create newline delimited json file from input tsv."""

    tsv_df = pd.read_csv(input_tsv, sep="\t")

    basename = input_tsv.split(".tsv")[0]
    output_filename = f"{basename}_recoded_newline_delimited.json"

    all_rows = []
    for index, row in tsv_df.iterrows():
        # recode the row json with gs:// path ingest format if applicable
        recoded_row_dict = create_recoded_json(row)
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