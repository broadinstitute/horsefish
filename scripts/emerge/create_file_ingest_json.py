import argparse
import pandas as pd


def create_file_ingest_json(input_file):
    """Create newline delimited json file for TDR bulk file ingest API."""

    ingest_files = pd.read_excel(input_file, sheet_name="Sheet1", index_col=None)

    output_basename = input_file.split(".xlsx")[0]
    output_json = open(f"{output_basename}_newline_delimited.json", "w")

    for row in ingest_files.iterrows():
        row[1].to_json(output_json)
        output_json.write("\n")

    print(f"Newline delimited json created: {output_json}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a newline delimited json for bulk ingesting data into a TDR dataset.')

    parser.add_argument('-x', '--excel', required=True, type=str, help='excel (.xlsx) file with file ingest information per file.')
    args = parser.parse_args()

    create_file_ingest_json(args.excel)
