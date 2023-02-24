## Test schema file setup ##
You can either:
- replace the contents of 'scripts/cfos/dataset_tables_schema.json' file with the contents of the 'scripts/cfos/tests/test_schema.json' file temporarily OR
- change the code in make_dataset_data_tables.py so that schema_json points to the test schema file instead of the real one

## How to Run ##
### test fail ### 
python3 make_dataset_data_tables.py -d schema1 -x tests/CFoS_Template_Test_Schema1_Fail.xlsx -p broad-cfos-data-platform1 -w cFOS_automation_testing

### test pass ###
python3 make_dataset_data_tables.py -d schema1 -x tests/CFoS_Template_Test_Schema1_Pass.xlsx -p broad-cfos-data-platform1 -w cFOS_automation_testing


## Validating outcomes ##
Check the validation_erros.csv and make sure expected outcomes are met. 
General expected outcomes are explained above the column ID in the Fail.xlsx file. 

Note that a second schema has been included, but test data needs to be made for it. You can edit the provided examples for schema1 to work for schema2 if you want to test using that schema as well. It involves moving columns around.