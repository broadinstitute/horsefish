import ingest as ingest
import unittest
import unit_test_data.test_data_models as models
import unit_test_data.expected_outputs as eo
import pandas as pd

# input tsv to original function to test
input_test_tsv = "unit_test_data/test_data.tsv"
# expected format of file generated from function
expected_newline_json = "unit_test_data/expected_ingest.json"

class TestNewlineJsonMethods(unittest.TestCase):

    # def test_update_header_name_with_entity_metadata(self):
    #     df = pd.read_csv(input_test_tsv, header=0,
    #              sep='\t')
    #     new_df = ingest.update_header_name(df, models.with_entity_id_metadata_instance["entity_metadata"])
    #     print(new_df)
    #     self.assertEqual(list(new_df.columns), eo.expect_columns_new_header)

    def test_select_grouped_columns(self):
        df = pd.read_csv(input_test_tsv, header=0,
                 sep='\t')
        # print(eo.Number_table_df)
        # print(eo.sojourn_zenith_table_df )
        group_cols = ingest.select_grouped_columns(df, models.instance["models"] )
        self.assertEqual(group_cols, eo.exp_column_groups)


if __name__ == '__main__':
    unittest.main()
