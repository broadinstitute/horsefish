import pickle
import weakref
import logging
import pandas as pd
import sys
import os

# Path to the pickle file
file_path = 'Synapse_seq_v1_data.pkl'
# Path to the output report file
report_file = 'dataframe_exploration_report.txt'

try:
    # Apply a monkey patch to handle compatibility issues with AnnData
    import anndata
    from anndata._core import file_backing

    # Store the original __setstate__ method
    original_setstate = file_backing.AnnDataFileManager.__setstate__

    # Define a patched __setstate__ method that handles missing '_adata_ref'
    def patched_setstate(self, state):
        try:
            # Try the original method first
            original_setstate(self, state)
        except KeyError as e:
            if str(e) == "'_adata_ref'":
                logging.info("Handling missing '_adata_ref' key...")
                # Create a modified state with a dummy _adata_ref
                modified_state = state.copy()
                # Create a dummy AnnData object
                dummy_adata = type('DummyAnnData', (), {})()
                modified_state['_adata_ref'] = dummy_adata
                # Set all attributes from the modified state
                self.__dict__ = modified_state.copy()
                # Set _adata_ref to a weakref to the dummy object
                self.__dict__['_adata_ref'] = weakref.ref(dummy_adata)
            else:
                # Re-raise if it's a different KeyError
                raise

    # Apply the monkey patch
    file_backing.AnnDataFileManager.__setstate__ = patched_setstate

    print("Monkey patch applied. Attempting to load the pickle file...")

    # Redirect stdout to the report file
    original_stdout = sys.stdout
    report = open(report_file, 'w')
    sys.stdout = report

    # Now try to load the pickle file
    with open(file_path, 'rb') as f:
        data = pickle.load(f)

    print("Successfully loaded the pickle file!")
    print("Type of data:", type(data))
    print("Keys:", list(data.keys()))

    # Examine each dataframe in the dictionary
    for key in data.keys():
        print(f"\n--- Examining '{key}' ---")
        item = data[key]
        print(f"Type: {type(item)}")

        # Check if it's a pandas DataFrame
        if isinstance(item, pd.DataFrame):
            print(f"Shape: {item.shape}")
            columns = list(item.columns)
            if len(columns) > 10:
                print(f"First 10 columns: {columns[:10]}")
            else:
                print(f"All columns: {columns}")
            print(f"Sample data (first 5 rows):")
        # Check if it's an AnnData object
        elif hasattr(item, 'var') and hasattr(item, 'obs'):
            print(f"AnnData object with shape: {item.shape}")
            if hasattr(item.var, 'columns'):
                var_columns = list(item.var.columns)
                if len(var_columns) > 10:
                    print(f"First 10 var columns: {var_columns[:10]}")
                else:
                    print(f"All var columns: {var_columns}")
            if hasattr(item.obs, 'columns'):
                obs_columns = list(item.obs.columns)
                if len(obs_columns) > 100:
                    print(f"First 10 obs columns: {obs_columns[:100]}")
                else:
                    print(f"All obs columns: {obs_columns}")
        else:
            print(item.keys())
            print(f"Not a standard dataframe. Available attributes: {dir(item)[:20]}")

except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")
    import traceback
    traceback.print_exc()
finally:
    # Restore stdout
    sys.stdout = original_stdout
    report.close()
    print(f"Report generated: {os.path.abspath(report_file)}")
