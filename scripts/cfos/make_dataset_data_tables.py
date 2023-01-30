"""Parse user supplied input to create data tables in Terra workspaces for the appropriate type/format of dataset.

Usage:
    > python3 make_dataset_data_tables.py -d DATASET_TYPE -x EXCEL_FILE -w TERRA_WORKSPACE_NAME -p TERRA_WORKSPACE_PROJECT 
"""
import argparse
import os
from firecloud import api as fapi
import json
import pandas as pd
from pandas_schema import*
from pandas_schema.validation import*
import validate
# from validate import DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA as schema_validator
from data_tables_class import cfos_data
# from dataset_custom_errors import EmptySchemaDefinitionException





def upload_dataset_table_to_workspace(tsv_filenames_list, workspace_name, workspace_project):
    """Upload each of the tables in the chosen dataset to Terra workspace."""

    for tsv in tsv_filenames_list:
        response = fapi.upload_entities_tsv(workspace_project, workspace_name, tsv, model='flexible')
        status_code = response.status_code

        table_name = tsv.split("_")[0]
        # TODO: do we want it to fail entirely as long as a single table fails to upload
        # if not success
        if status_code != 200:
            print(f"Failed: {table_name} table has failed to upload. Please see errors: {response.text}")
            return

        # if success
        print(f"Success: {table_name} has been uploaded to {workspace_project}/{workspace_name}.")

    print("Success: All data tables have been loaded to workspace.")


def modify_file_table_dataframe(file_table_df):
    """File table needs additional modification."""

    file_types = ["features", "matrix", "barcode", "summary"]  # four types of files
    file_type_dataframes = []  # empty list to hold re-organized dataframe per file type

    # for each file_path type, create a separate dataframe
    for file_type in file_types:
        # list of columns to subset + the additional file_type column
        cols_to_subset = ["library_id", "donor_id", "DataModality", "biosample_id"]
        cols_to_subset.append(f"{file_type}_file_path")

        # make file type specific dataframe and rename column to generic name
        file_type_df = file_table_df[cols_to_subset]
        file_type_df = file_type_df.rename(columns={f"{file_type}_file_path": "file_path"})

        # create the file_type column and the file_id column (as concatenation of other values)
        # TODO: determine if concatenation of columns is unique or if other file identifier is required
        # TODO: parsing file paths may not be a safe automation across datasets
        file_type_df["file_type"] = file_type
        file_type_df["file_id"] = file_type_df["file_type"] + "_" + file_type_df["donor_id"] + "_" + file_type_df["library_id"]

        # add dataframe to list
        file_type_dataframes.append(file_type_df)

    # concatenate all the file_type dataframes into one and return
    modified_file_table_df = pd.concat(file_type_dataframes)
    return modified_file_table_df


def generate_load_table_files(dataset_tables_dict):
    """Generate load table tsv files for each of the dataset tables."""

    # instantiate empty list to hold names of generated tsv load files
    tsv_filenames_list = []

    # for each table in dictionary
    for table_name in dataset_tables_dict:
        table_df = dataset_tables_dict[table_name]
        if table_name == "file":
            table_df = modify_file_table_dataframe(table_df)
        # change table name column to new name --> entity:table_name_id and set as index
        table_df = table_df.rename(columns={f"{table_name}_id": f"entity:{table_name}_id"})
        table_df.set_index(f"entity:{table_name}_id", inplace=True)

        # set output tsv filename
        output_filename = f"{table_name}_table_load_file.tsv"
        tsv_filenames_list.append(output_filename)

        # write out tsv file per table
        table_df.to_csv(output_filename, sep="\t")

    print(f"Success: Dataset table load .tsv files have been generated.")
    return tsv_filenames_list


def create_dataset_tables_dictionary(validated_dataset, dataset_name, dataset_table_names, schema_dict):
    """For a given dataset, subset/separate input columns into individual table dataframes."""

    # instantiate empty dictionary to capture {table_name: table_metadata_dataframe}
    dataset_tables_dict = {}

    # create a subset df for each table defined in the chosen dataset
    for table in dataset_table_names:
        # get subset dataframe for specific table for given dataset name
        table_df = dataset_metadata[schema_dict[dataset_name][table]]

        # TODO: edit the value = dataframe for a table to have the entity:table_name_id format

        # add KVP {table name : table_df} entry to all tables dictionary
        dataset_tables_dict[table] = table_df

    print(f"Success: Dataset has been partitioned into {len(dataset_table_names)} tables: {dataset_table_names}")
    return dataset_tables_dict


def validate_dataset(data, field_definition_dict):
    # Validate the full dataset metadata with Schema validator.

    # Build validation based on field definitions dictionary

    """ # capture errors from the validation against the pre-defined validations per column
    errors = schema_validator.validate(dataset, columns=schema_validator.get_column_names())
    # get indices of the failed rows
    errors_index_rows = [e.row for e in errors]

    # if any errors, print error message and exit
    if errors_index_rows:
        print(f"Failed: Dataset validation errors found. Please retry after correcting errors listed in validation_errors.csv.")
        pd.DataFrame({'validation_error':errors}).to_csv('validation_errors.csv')
        exit()

    # TODO: determine how to handle drops of failed rows/columns to get cleaned data
    # errors_index_rows = [e.row for e in errors]
    # data_clean = dataset_metadata_df.drop(index=errors_index_rows, axis=1)
    # data_clean.to_csv('clean_data.csv')
    print(f"Success: Dataset has been validated.")
    return dataset """


def load_excel_input(excel, field_definitions_dict):
    # Read excel file input into a dataframe and validate for required columns.

    try:
        raw_dataset_df = pd.read_excel(excel, sheet_name=None, skiprows=2, index_col=None)
        
    # TODO: Add better file error handling    
    except ValueError as e:
        print(e)
        exit()

    return flatten_dataframe_columns(raw_df=raw_dataset_df, field_definitions_dict=field_definitions_dict)

def flatten_dataframe_columns(raw_df, field_definitions_dict):
    all_columns_df = pd.DataFrame()
    
    for sheet in list(raw_df.keys()):
        for column_name in list(raw_df[sheet].keys()):
            if column_name in list(field_definitions_dict.keys()):
                all_columns_df[column_name] = raw_df[sheet][column_name]
                
    print("Success: Excel file has been loaded into a dataframe.")
    return all_columns_df


"""
    for table_name in dataset_table_names:
        try:
            dataset_metadata_df = pd.read_excel(excel, sheet_name=table_name, skiprows=2, usecols=schema_dict[table_name], index_col=None)
            print(dataset_metadata_df)

        except ValueError as e:
            # if the value error is about missing columns in input file
            if len(e.args) > 0 and e.args[0].startswith("Usecols do not match columns, columns expected but not found"):
                missing_cols = e.args[0].split(":")[1]
                print(f"Failed: Input excel file sheet {table_name} has the following missing columns that are required: {missing_cols}")
            else:
                print(e)
            exit()
    
    print("Success: Excel file has been loaded into a dataframe.")
    return dataset_metadata_df
"""

"""
def get_expected_columns_list(schema_json, dataset_name):
    # Get list of expected columns in excel based on selected dataset schema.

    # read schema into dictionary
    with open(schema_json) as schema:
        schema_dict = json.load(schema)

    # get list of tables for a given dataset name
    dataset_table_names = list(schema_dict["schema_definitions"][dataset_name].keys())

    # get all cols (attributes) from all tables for dataset
    # returns nested list of lists with column names
    nested_dataset_cols = [schema_dict["schema_definitions"][dataset_name][table] for table in dataset_table_names]
    # unnest for flat list of columns to parse from excel file
    expected_dataset_cols = [col for table_cols in nested_dataset_cols for col in table_cols]

    print("Success: Expected list of columns has been determined.")
    return expected_dataset_cols, dataset_table_names, schema_dict["schema_definitions"][dataset_name]
"""

def load_config_file(config_json, schema_type):
    if schema_type is None or schema_type == "":
        schema_type = "all_fields"

    with open(config_json) as config:
        config_dict = json.load(config)

    fields = get_config_field_dict(config_dict=config_dict)
    schemas = get_config_schema_dict(config_dict=config_dict, schema_type=schema_type)

    return schemas, fields

def get_config_field_dict(config_dict):
    return config_dict["fields"]

def get_config_schema_dict(config_dict, schema_type):
    if schema_type is None or str.strip(schema_type) == "":
        field_list = list(config_dict["schema_definitions"].keys())
        return {"data" : field_list}

    # print("schema: ")
    # print(config_dict["schema_definitions"][schema_type])
    
    return config_dict["schema_definitions"][schema_type]
    
    """ try:
        schema_dict = config_dict["schema_definitions"][schema_type]
        if schema_dict is None or len(list(schema_dict.keys())) < 1:
            raise EmptySchemaDefinitionException

    except EmptySchemaDefinitionException:
        quit() """


def extract_config_fields(raw_df, field_dict):
    processed_data_df = pd.DataFrame()

    # if field exists in the data frame, add it to the new data frame
    for table in raw_df:
        for field in table:
            if field in list(field_dict.keys()):
                processed_data_df[field] = raw_df[table][field]
    # if field doesn't exist in the data frame, skip it for now
    return processed_data_df

def flatten_df_one_level(df):
    flat_df = pd.DataFrame()
    for table in df:
        pd.concat(flat_df, table)
    
    return flat_df


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create, validate, and upload (optionally ) Terra dataset specific data tables to a Terra workspace.')

    # REK TODO: update so JSON is assumed to live in an internal directory location and not as an input file. 
    # Goal: allow qualified people to make easy updates to schemas, but don't encourage random users to play with it.

    parser.add_argument(
        '-d', 
        '--dataset_name', 
        required=False, 
        default='inph', 
        type=str, 
        help='Dataset type to apply validations and data table structure. ex. proteomics, atac-seq, transcriptomics, singe-cell'
    )
    parser.add_argument(
        '-x', 
        '--excel', 
        required=False, 
        default=os.path.expanduser('~/Documents/CFoS_Multi_Sheet.xlsx'), 
        type=str, 
        help='CFoS data excel file (.xlsx) - based on CFoS data intake template file.'
    )
    parser.add_argument(
        '-j', 
        '--schema', 
        required=False, 
        default=os.path.expanduser('~/Projects/horsefish/scripts/cfos/dataset_tables_schema.json'), 
        type=str, 
        help='Dataset schema file (.json) - defines per dataset tables and associated attributes.'
    )
    parser.add_argument(
        '-p', 
        '--project', 
        required=False, 
        type=str, 
        help='Terra workspace project (namespace).'
    )
    parser.add_argument(
        '-w', 
        '--workspace', 
        required=False, 
        type=str, 
        help='Terra workspace name.'
    )
    parser.add_argument(
        '-v', 
        '--validate', 
        required=False, 
        action='store_true', 
        help='Set parameter to run only validation on input excel file.'
    )
    parser.add_argument(
        '-u', 
        '--upload', 
        required=False, 
        action='store_true', 
        help='Set parameter to upload data table files to workspace. default = no upload'
    )
    args = parser.parse_args()

    # parse schema using dataset name to get list of expected columns
    # expected_dataset_cols, dataset_table_names, schema_dict = get_expected_columns_list(args.schema, args.dataset_name)
    schema_definition_dict, field_definition_dict = load_config_file(schema_type='inph', config_json=args.schema)
    
    # load excel to dataframe validating to check if all expected columns present
    dataset_df = load_excel_input(args.excel, field_definitions_dict=field_definition_dict)
    # validate_data_frame(field_definition_dict=field_definition_dict, data_df=dataset_df)
    print("data:")
    print(dataset_df)
    print("")
    field_validation_list = validate.create_field_validation_list(field_dict=field_definition_dict,data_dict=dataset_df)
    print("Validation:")
    print(field_validation_list)
    validation_build_dict = validate.create_validation_build_dict(fields_dict=field_definition_dict, fields_to_validate_list=field_validation_list)
    print("")
    print("validation build dictionary:")
    print(validation_build_dict)
    validate.dynamically_validate_df(data_df=dataset_df, field_dict=field_definition_dict)





    """ 
    # if validation flag, only validate
    if args.validate:
        validated_dataset = validate_dataset(dataset_metadata)
    else:
        validated_dataset = validate_dataset(dataset_metadata)
        dataset_tables_dict = create_dataset_tables_dictionary(validated_dataset, args.dataset_name, dataset_table_names, schema_dict)
        tsv_filenames_list = generate_load_table_files(dataset_tables_dict)
    
    # if upload flag, upload generated tsv files to Terra workspace
    if args.upload and args.workspace and args.project:
        upload_dataset_table_to_workspace(tsv_filenames_list, args.workspace, args.project)
    """
    # python3 make_dataset_data_tables.py -d inph -x test_data/CFoS_Template.xlsx -j dataset_tables_schema.json -p broad-cfos-data-platform1 -w cFOS_automation_testing 