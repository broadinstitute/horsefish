import filecmp
from one_step_data_ingest import create_newline_delimited_json
import unittest

# input tsv to original function to test
input_test_tsv = "unit_test_data/unit_test_input_file.tsv"
# expected format of file generated from function
expected_newline_json = "unit_test_data/expected_ingest.json"

class TestNewlineJsonMethods(unittest.TestCase):

    def test_create_newline_delimited_json(self):
        generated_newline_json = create_newline_delimited_json(input_test_tsv)

        with open(generated_newline_json, 'r') as generated:
            generated_json = generated.readlines()
        
        with open(expected_newline_json, 'r') as expected:
            expected_json = expected.readlines()
        
        assert expected_json == generated_json


if __name__ == '__main__':
    unittest.main()