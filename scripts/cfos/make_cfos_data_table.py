
"""Parse input data file, create CFoS data tables, and upload to Terra workspace.

Usage:
    > python3 make_cfos_data_table.py -t TSV_FILE -w TERRA_WORKSPACE_NAME -p TERRA_WORKSPACE_PROJECT """
import pandas as pd

# define column names that belong to each of the 4 data tables
DATA_TABLE_DEFINITIONS = {
    'Donor': ['age_at_biopsy', 'APOE_atDonorAge', 'APOE_value', 'follow_up_years', 'has_disease', 'has_phenotypic_sex',
              'MMSE_Biopsy_atDonorAge', 'MMSE_Biopsy_value', 'MMSE_final_atDonorAge', 'MMSE_final_value', 'neuropathology'],
    'BioSample': ['donor_id', 'hasAnatomicalSite', 'hasDonorAge'],
    'File': ['biosample_id', 'DataModality', 'donor_id', 'file_path', 'file_type', 'library_id'],
    'Library': ['comment', 'donor_id', 'UMI_threshold']
}


def create_data_tables(input_metadata):
    """Parse input metadata file and create four data table tsv files."""

    # from template: sheet1, skip non-column header rows, ignore first empty column
    all_metadata_df = pd.read_excel(input_metadata, sheet_name="Sheet1", skiprows=2, usecols="B:W", index_col=None)

    biosample_df = all_metadata_df[data_table_definitions["BioSample"]]
    # have to parse four columns into single column like format of File table
    # file_df = all_metadata_df[data_table_definitions["File"]]
    library_df = all_metadata_df[data_table_definitions["Library"]]
    donor_df = all_metadata_df[data_table_definitions["Donor"]]





if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parse CFoS data file to create and optionally upload Terra data tables to a Terra workspace.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='CFoS data file - based on CFoS data intake template file.')
    parser.add_argument('-p', '--project', required=True, type=str, help='Terra workspace project (namespace).')
    parser.add_argument('-w', '--workspace', required=True, type=str, help='Terra workspace name.')
    parser.add_argument('-u', '--upload', required=False, action='store_true', help='Set parameter to upload data table files to workspace.')

    create_data_tables(args.tsv)
