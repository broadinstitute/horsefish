# imports and environment variables
import pandas as pd
from firecloud import api as fapi
from scripts.fiss_fns import call_fiss


# Be able to pull subset from columnn name
# Grab data from workspace 
# Send to TDR

CULTURE_TABLE_COLUMNS = [ "culture_id", "organism"]
PROJECT = "broad-firecloud-dsde"
WORKSPACE = "learning"

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

  col_dicts = {}
  chunksize = 10000  # Adjust chunk size as needed (experiment for optimal value)

  for chunk in df.itertuples(chunksize=chunksize):
    # Efficiently select columns for each grouping using list comprehension
    group_cols = {name: chunk[[col for col in cols]] for name, cols in col_groupings.items()}

    # Create dictionaries directly from grouped columns with index
    col_dicts.update({name: {idx: dict(zip(cols, row[cols])) for idx, row in zip(chunk.index, group[cols])}
                       for name, group in group_cols.items()})

  return col_dicts


def main():
  col_groupings = {
    "CULTURE_TABLE": [ "culture_id", "organism"],  # Dictionary named for Culture Table where the keys are the name of the columns
  }
  data = call_fiss(fapi.get_entities_tsv, 200, PROJECT, WORKSPACE,  "sample", model = "flexible")
  # Get all of the data from the student table and load into a pandas dataframe
  print(data)
  df = pd.read_csv(io.StringIO(data.text), sep='\t')
  df.set_index('entity:sample_id', inplace = True)


  result_dicts = df_to_col_dicts_chunked(df.copy(), col_groupings)

  print(result_dicts["CULTURE_TABLE"])

if __name__ == '__main__':
    main()
