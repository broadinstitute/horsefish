"""Validate dataframes with validators."""
from pandas_schema import*
from pandas_schema.validation import*
import re

# validators
# column values cannot be null
null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]


df = pd.DataFrame({
'prospect': ['Bob', 'Jill', 'Steve', 'Mary'], 
'value': [10000, 15000, 500, 50000],
'workflow_entry_step': ['New', 'In Progress', 'Closed', 'In Progress'], 
'workflow_next_step': ['In Progress', 'Closed' , None, 'In Progress']})

print(df)

schema = Schema([
   Column('prospect', [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()]),
   Column('value', [CanConvertValidation(float)]),
   Column('workflow_entry_step', []),
   Column('workflow_next_step', [MatchesPatternValidation("^In")
         ])
])
errors = schema.validate(df)
for error in errors:
    print(error)

"""
   Column('workflow_next_step', [CustomSeriesValidation(lambda x: x != df['workflow_entry_step'], 'Steps cannot be the same.'),
   InListValidation([None,'New','In Progress','Closed'])]) 
"""

#build the string and then execute

"""




# schema validator definition
# TODO: set required columns
# validation schema by specific column name across any and all possible datasets - not specific to just a single dataset
DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA = Schema([
   Column("donor_id", null_validation),
   Column("hasDonorAge", [IsDtypeValidation(int)]),
   Column("has_phenotypic_sex", [InListValidation(has_phenotypic_sex_options)]),
   Column("age_at_biopsy", [IsDtypeValidation(int)]),
   Column("hasOrganismType", [InListValidation(has_organism_type_options)]),
   # TODO: are there a list of sites, or is this a free form field
   #    Column("hasAnatomicalSite", [None])
   Column("neuropathology", [InListValidation(neuropathology_options)]),
   Column("biosample_id", [IsDistinctValidation()]),
   Column("hasBioSampleType", [InListValidation(has_biosample_type_options)]),
   Column("MMSE_Biopsy_value", [IsDtypeValidation(int)]),
   Column("MMSE_Biopsy_atDonorAge", [IsDtypeValidation(int)]),
   Column("MMSE_Final_value", [IsDtypeValidation(int)]),
   Column("MMSE_Final_atDonorAge", [IsDtypeValidation(int)]),
   Column("APOE_value", [IsDtypeValidation(int)]),
   Column("APOE_atDonorAge", [IsDtypeValidation(int)]),
   Column("DataModality", [InListValidation(data_modality_options)]),
   # file path columns must start with "gs://"
   # TODO: check starts with not just for pattern
   Column("summary_file_path", [MatchesPatternValidation("gs://")]),
   Column("features_file_path", [MatchesPatternValidation("gs://")]),
   Column("matrix_file_path", [MatchesPatternValidation("gs://")]),
   Column("barcode_file_path", [MatchesPatternValidation("gs://")]),
   Column("biosample_id_of_file", null_validation),
   Column("library_id", null_validation),
   Column("UMI_threshold", [IsDtypeValidation(int)]),
   Column("follow_up_years", [IsDtypeValidation(int)]),
   Column("has_disease", [InListValidation(has_disease_options)])
   #    Column("comment", [None])
]) 
                                            """


def create_field_validation_list(field_dict, data_dict):
   field_dict_keys_list = list(field_dict.keys())
   validation_fields = set()

   for field_id in field_dict_keys_list:
      if required_field(field_id, field_dict) or field_exists_in_df(field_id=field_id, df=data_dict):
         validation_fields.add(field_id)

   return validation_fields
      
def field_exists_in_df(field_id, df):
    if field_id in list(df.keys()):
        return True
    return False

def required_field(field_id, field_dict):
   field_id_keys = list(field_dict[field_id].keys())

   if get_optional_field_key() not in field_id_keys:
      return True

   if field_attribute_is_true(attribute_to_check=get_optional_field_key(), field_id=field_id, field_dict=field_dict):
      return False

   return True


def field_attribute_is_true(field_id, field_dict, attribute_to_check):
   # handle null defaults to "true" on a field-by-field basis outside this function
   if attribute_to_check not in list(field_dict[field_id].keys()):
      print(f"attribute {attribute_to_check} not in field key list")
      return False

   if field_dict[field_id][attribute_to_check] == "True":
      print(f"attribute {attribute_to_check} is true")
      return True
   print(f"attribute {attribute_to_check} not true")
   return False

def field_attribute_value_exists(field_id, field_dict, attribute_to_check):
   if attribute_to_check not in list(field_dict[field_id].keys()):
      return False

   attribute_value = str(field_dict[field_id][attribute_to_check]).strip()
   if attribute_value is None or attribute_value == "":
      return False

   return True

def check_field_type(field_id, field_dict, expected_type):
   type_key = get_field_type_key()
   if type_key not in list(field_dict[field_id].keys()):
      print(f"{type_key} is not in the list of keys for {field_id}")
      return False

   field_type = field_dict[field_id][type_key]

   print(f"{field_type} should match {expected_type}")

   if field_type == expected_type:
      return True

   return False

def is_file_path_field_type(field_id, field_dict):
   return check_field_type(field_id=field_id,field_dict=field_dict, expected_type=get_file_path_type_val())

def is_numeric_field_type(field_id, field_dict):
      return check_field_type(field_id=field_id, field_dict=field_dict, expected_type=get_numeric_field_type_val())

def has_valid_pattern_match_value(field_id, field_dict):
   # right now, all file paths have a default pattern to match if none is provided
   if is_file_path_field_type(field_id=field_id, field_dict=field_dict):
      return True

   return field_attribute_value_exists(field_id=field_id, field_dict=field_dict, attribute_to_check=get_field_pattern_to_match_key())


def has_category_value_validation(field_id, field_dict):
   return field_attribute_value_exists(
      field_id=field_id,
      field_dict=field_dict,
      attribute_to_check=get_allowed_values_key()
   )


def only_int_values_allowed(field_id, field_dict):
   if not is_numeric_field_type(field_id=field_id, field_dict=field_dict):
      return False

   return field_attribute_is_true(field_id=field_id, field_dict=field_dict, attribute_to_check=get_int_only_val_key())

def create_validation_build_dict(fields_dict, fields_to_validate_list):
   dynamic_validation_build_dict = {}

   for field_id in fields_to_validate_list:
      # TODO: add explicit field type validation later
      if null_value_invalid(field_id=field_id, fields_dict=fields_dict):
         add_null_validation(
            field_id=field_id,
            validation_dict=dynamic_validation_build_dict
         )
      if has_valid_pattern_match_value(field_id=field_id, field_dict=fields_dict):
         add_matches_pattern_validation(
            field_id=field_id, 
            field_dict=fields_dict,
            validation_dict=dynamic_validation_build_dict
         )
      if only_int_values_allowed(field_id=field_id, field_dict=fields_dict):
         add_integer_only_validation(
            field_id=field_id,
            validation_dict=dynamic_validation_build_dict
         )
      if has_category_value_validation(field_id=field_id, field_dict=fields_dict):
         add_category_value_validation(
            field_id=field_id,
            field_dict=fields_dict,
            validation_dict=dynamic_validation_build_dict
         )
      """
      if must_be_unique_column():
         add_unique_validation(
            field_id=field_id,
            validation_dict=dynamic_validation_build_dict
         )
      """

   return dynamic_validation_build_dict      


def null_value_invalid(fields_dict, field_id):
   field_id_keys = list(fields_dict[field_id].keys())
   value_required_key = get_value_required_key()

   if value_required_key not in field_id_keys:
      return False

   elif field_attribute_is_true(attribute_to_check=value_required_key, field_id=field_id, field_dict=fields_dict):
      return True

   return False


def add_validation(validation_dict, field_id, validation_string_to_add):
   if field_id not in list(validation_dict.keys()):
      validation_dict[field_id] = []

   validation_dict[field_id].append(validation_string_to_add)


def add_null_validation(validation_dict, field_id):
   validation = "null_validation"
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_string_to_add=validation)


def add_matches_pattern_validation(validation_dict, field_dict, field_id):
   """
   TODO: 
   this is somewhat redundant with the has_valid_pattern_match_logic function;
   should look into whether there's a way to consolidate this logic more
   """
   pattern = str()
   if field_attribute_value_exists(field_id=field_id, field_dict=field_dict, attribute_to_check=get_field_pattern_to_match_key()):
      pattern = str(field_dict[field_id][get_field_pattern_to_match_key()]).strip()

   # TODO: allow JSON to specify custom default patterns for file_path fields
   if (pattern is None or str(pattern).strip() == "")\
      and is_file_path_field_type(field_id=field_id,field_dict=field_dict):
      pattern = "^gs://"
   
   validation = f"MatchesPatternValidation(\"{pattern}\")"
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_string_to_add=validation)


def add_category_value_validation(validation_dict, field_id, field_dict):
   if not field_attribute_value_exists(
      field_id=field_id, 
      field_dict=field_dict,
      attribute_to_check=get_allowed_values_key()
   ):
      quit()

   allowed_values_list = field_dict[field_id][get_allowed_values_key()]

   if allowed_values_list is None or len(allowed_values_list) < 1:
      print("Bad allowed values list")
   
   validation = f"InListValidation({allowed_values_list})"
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_string_to_add=validation)

def add_integer_only_validation(validation_dict, field_id):
   validation = "IsDtypeValidation(int)"
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_string_to_add=validation)

""" def add_valid_range(validation_dict, field_id, min, max):
   # waiting for use case before instantiating this, but here's the idea --> 
   if min is None or max is None:
      quit()
   
   validation = f"InRangeValidation({min}, {max})"
   validation_dict[field_id].append(validation) """




def dynamically_validate_df(data_df, field_dict):
   fields_to_validate_list = create_field_validation_list(data_dict=data_df, field_dict=field_dict)

   dynamic_validation_build_guide_dict = create_validation_build_dict(fields_dict=field_dict, fields_to_validate_list=fields_to_validate_list)
   validation_code = create_validation_code_from_logic(validation_build_guide_dict=dynamic_validation_build_guide_dict)
   DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA = validation_code
   errors = DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA.validate(df)
   for error in errors:
      print(error)


def create_validation_code_from_logic(validation_build_guide_dict):
   validation_code_beginning = "Schema(["
   validation_code_end = "])"
   columns_logic_list = []
   
   for column_id in list(validation_build_guide_dict.keys()):
      validation_logic_list = validation_build_guide_dict[column_id]
      validation_logic_string = ", ".join(validation_logic_list)
      columns_logic_list.append(f"Column(\"{column_id}\", [{validation_logic_string}])")
   
   complete_validation_code = validation_code_beginning + ", ".join(columns_logic_list) + validation_code_end
   print(complete_validation_code)
   return complete_validation_code



def create_schema_validation(field_dict):
   validation_dict = {}
   for field_id in list(field_dict.keys()):
      validation_dict[field_id]["validations"] = []
      validation_dict[field_id]["required"] = True
      populate_validation_build_dict(field_dict=field_dict, field_id=field_id, validation_dict=validation_dict)

def populate_validation_build_dict(field_dict, field_id, validation_dict):
   field_attributes = list(field_dict[field_id].keys())
   if "field_type" not in field_attributes:
      quit() # create "no field type error"
   

def create_column(field_id):
   return "Column(" + str(field_id) + ","



# Constants

# Keys
def get_optional_field_key():
   return "optional"

def get_value_required_key():
   return "value_required"

def get_int_only_val_key():
   return "integer_only"

def get_field_type_key():
   return "field_type"

def get_field_pattern_to_match_key():
   return "pattern_to_match"

def get_allowed_values_key():
   return "allowed_values"


# Values

def get_file_path_type_val():
   return "file_path"

def get_numeric_field_type_val():
   return "number"

def get_category_field_type_val():
   return "category"

def get_file_path_field_type_val():
   return "file_path"