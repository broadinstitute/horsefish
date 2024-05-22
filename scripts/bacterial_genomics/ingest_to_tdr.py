# imports and environment variables
import io
import sys
from collections.abc import Iterable
import pandas as pd
from firecloud import api as fapi

from pathlib import Path
path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))
from scripts.emerge.ingest_to_tdr import call_ingest_dataset

# Be able to pull subset from columnn name
# Grab data from workspace
# Send to TDR

CULTURE_TABLE_COLUMNS = ["culture_id", "organism"]

PROJECT = "broad-firecloud-dsde"
WORKSPACE = "learning"

# DEVELOPER: update this field anytime you make a new docker image and update changelog
version = "1.0"


def get_entities_tsv(fapifunc, okcode, *args, specialcodes=None, **kwargs):
    ''' call FISS (firecloud api), check for errors, return json response

    function inputs:
        fapifunc : fiss api function to call, e.g. `fapi.get_workspace`
        okcode : fiss api response code indicating a successful run
        specialcodes : optional - LIST of response code(s) for which you don't want to retry
        *args : args to input to api call
        **kwargs : kwargs to input to api call

    function returns:
        response : non-parsed API response if you submitted specialcodes

    example use:
        output = call_fiss(fapi.get_workspace, 200, 'help-gatk', 'Sequence-Format-Conversion')
    '''
    # call the api
    response = fapifunc(*args, **kwargs)
    # print(response.status_code)

    # check for errors; this is copied from _check_response_code in fiss
    if type(okcode) == int:
        # codes = [okcode]
        if specialcodes is None:
            codes = [okcode]
        else:
            codes = [okcode]+specialcodes
    if response.status_code not in codes:
        print(response.content)
        raise ferrors.FireCloudServerError(
            response.status_code, response.content)
    elif specialcodes is not None:
        return response
    # return the json response if all goes well
    return response


def df_to_col_dicts_chunked(df, col_groupings):
    """
    Efficiently creates multiple dictionaries from a large DataFrame,
    using chunking for memory management. Each dictionary contains specific
    column combinations you define.

    Args:
        df (pandas.DataFrame): The DataFrame to process.
        col_groupings (dict): A dictionary specifying column combinations.
            Keys are desired names for the dictionaries, and values are lists
            containing column names for each dictionary.

    Returns:
        dict: A dictionary where keys are the provided names (from col_groupings)
              and values are dictionaries containing the selected columns.
    """
    col_dicts = create_deeper_nested_dicts(col_groupings)
    print(f"Col_dicts is: {col_dicts}")

    for chunk in df:
        print("Where getting another chunk")
        
        # Efficiently select columns for each grouping using list comprehension
        group_cols = select_grouped_columns(chunk, col_groupings)
        print(f"Group columns are: {group_cols}")

        for name, group in group_cols.items():
            # col_dicts[name].extend(group.to_dict(orient='list'))
            print(f"group means: {group}")
            print(f"first time dictionary looks like this: {col_dicts}")
            print(f"Name is {name}")
            print(f"Trying to get items: {group.to_dict(orient='list').items()}")
            # Create dictionary directly using DataFrame index)
            for column, column_values in group.to_dict(orient='list').items():
                print(f"in column:{column} and column_values:{column_values}")
                col_dicts[name][column].extend(column_values)
            print(f"dictionary looks like this: {col_dicts}")
    return col_dicts

def create_deeper_nested_dicts(original_dict):
  """
  Creates a new dictionary with even deeper nested dictionaries based on the values of the original dictionary.

  Args:
      original_dict: A dictionary where values are dictionaries.

  Returns:
      A new dictionary where keys are the same as the original dictionary, and values are dictionaries with keys
      being the original inner dictionary keys and values as empty lists.
    # Example usage
        original_dict = {"key1": {"color": "red", "shape": "round"}, "key2": {"fruit": "banana"}}
        new_dict = create_deeper_nested_dicts(original_dict)

        print(new_dict)
        # Output: {'key1': {'color': [], 'shape': []}, 'key2': {'fruit': []}}
  """
  new_dict = {}
  for key, inner_dict in original_dict.items():
    deeper_dict = {inner_key: [] for inner_key in inner_dict}
    new_dict[key] = deeper_dict
  return new_dict

def select_grouped_columns(chunk, col_groupings):
    """
    Selects specific columns from a DataFrame chunk based on grouping definitions.

    Args:
        chunk (pandas.DataFrame): A chunk of the DataFrame being processed.
        col_groupings (dict): A dictionary specifying column combinations.
            Keys are desired names for the dictionaries, and values are lists
            containing column names for each dictionary.

    Returns:
        dict: A dictionary where keys are names from col_groupings and values are
              DataFrames containing only the selected columns for each group.
    """

    group_cols = {}
    for name, cols in col_groupings.items():
        # Regular loop to select columns for each group
        # Select columns using list of column names
        group_cols[name] = chunk[cols]
    return group_cols

def main():
    col_groupings = {
        # Dictionary named for Culture Table where the keys are the name of the columns
        "Culture": CULTURE_TABLE_COLUMNS,
    }
    response = get_entities_tsv(fapi.get_entities_tsv, 200, PROJECT,
                                WORKSPACE, "sample", model="flexible")
    # Get all of the data from the student table and load into a pandas dataframe
    # convert response to text and turn into df
    df = pd.read_csv(io.StringIO(response.text), header=0,
                     iterator=True, sep='\t', chunksize=2)
    result_dicts = df_to_col_dicts_chunked(df, col_groupings)
    # for table_name, table_data in result_dicts.items():
    #     print(table_name)
    #     print(table_data)
        # call_ingest_dataset(table_data.copy(), table_name, "f5b28bfa-3a7e-4fa9-b966-283d364499df")


if __name__ == '__main__':
    main()
