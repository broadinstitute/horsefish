## Test schema file setup ##
You can run the code using the test schema by adding the parameter "-t True" to run in testing mode (examples shown below in How to Run section)

## How to Run ##
### test fail ### 
python3 make_dataset_data_tables.py -d schema1 -x tests/CFoS_Template_Test_Schema1_Fail.xlsx -p broad-cfos-data-platform1 -w cFOS_automation_testing -t True

### test pass ###
python3 make_dataset_data_tables.py -d schema1 -x tests/CFoS_Template_Test_Schema1_Pass.xlsx -p broad-cfos-data-platform1 -w cFOS_automation_testing -t True


## Validating outcomes ##
Check the validation_erros.csv and make sure expected outcomes are met. 
General expected outcomes are explained above the column ID in the Fail.xlsx file. 

The code can fail in the following ways:
- **primary key column contains non-unique values**: 'validation_error':{row: 6, column: "id_unique"}: "X1234B" contains values that are not unique
- **value doesn't match an expected pattern** : 'validation_error':{row: 6, column: "file_path_explicit_pattern"}: "gs://456" does not match the pattern "^fs://"
- **value is an unexpected type**. If "number", it must be a float. If "integer", it must be an integer: 'validation_error':The column integer_only_field has a dtype of object which is not a subclass of the required type <class 'int'>
- **category field value is not in list of allowed values**: 'validation_error':{row: 2, column: "category"}: "value12" is not in the list of legal options (value1, value2, value3, value4)
- **column listed in schema table is not included in the data**: 'validation_error':The column field_not_in_data exists in the schema but not in the data frame


Note that a second schema has been included, but test data needs to be made for it. You can edit the provided examples for schema1 to work for schema2 if you want to test using that schema as well. It involves moving columns around.