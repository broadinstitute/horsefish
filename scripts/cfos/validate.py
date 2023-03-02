"""Validate dataframes with validators."""
from pandas_schema import*
from pandas_schema.validation import*
import re

# validators
# column values cannot be null
#null_validation = CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')
null_validation = CustomSeriesValidation(lambda d: d.notna(), 'this field cannot be null')


# Keys
OPTIONAL_FIELD_KEY = "optional"
VALUE_REQUIRED_KEY = "value_required"
INT_ONLY_VAL_KEY = "integer_only"
FIELD_TYPE_KEY = "field_type"
PATTERN_TO_MATCH_KEY = "pattern_to_match"
ALLOWED_VALUES_KEY = "allowed_values"
IS_UNIQUE_KEY = "is_unique"

# Values
FILE_PATH_FIELD_TYPE_VAL = "file_path"
NUMERIC_FIELD_TYPE_VAL = "number"


# Generic functions used to check attributes
def field_attribute_value_exists(field_id, field_dict, attribute_to_check):
   if attribute_to_check not in field_dict[field_id]:
      return False

   attribute_value = str(field_dict[field_id][attribute_to_check]).strip()
   return attribute_value is not None and attribute_value != ""


def check_field_type(field_id, field_dict, expected_type):
   type_key = FIELD_TYPE_KEY
   if type_key not in field_dict[field_id]:
      return False

   field_type = field_dict[field_id][type_key]
   return field_type == expected_type


# Functions to check what validations should be applied
def has_valid_pattern_match_value(field_id, field_dict):
   # right now, all file paths have a default pattern to match if none is provided
   return check_field_type(field_id=field_id, field_dict=field_dict, expected_type=FILE_PATH_FIELD_TYPE_VAL)\
      or field_attribute_value_exists(field_id=field_id, field_dict=field_dict, attribute_to_check=PATTERN_TO_MATCH_KEY)


def has_category_value_validation(field_id, field_dict):
   return field_attribute_value_exists(
      field_id=field_id,
      field_dict=field_dict,
      attribute_to_check=ALLOWED_VALUES_KEY
   )


def only_int_values_allowed(field_id, field_dict):
   return check_field_type(field_id, field_dict, NUMERIC_FIELD_TYPE_VAL)\
      and INT_ONLY_VAL_KEY in field_dict[field_id]\
      and field_dict[field_id][INT_ONLY_VAL_KEY] == "True"


def null_value_invalid(fields_dict, field_id):
   return VALUE_REQUIRED_KEY in fields_dict[field_id] \
      and VALUE_REQUIRED_KEY in fields_dict[field_id]\
      and fields_dict[field_id][VALUE_REQUIRED_KEY] == "True"
   

# Functions to add validation to schema
def add_validation(validation_dict, field_id, validation_object_to_add):
   if field_id not in list(validation_dict.keys()):
      validation_dict[field_id] = []

   validation_dict[field_id].append(validation_object_to_add)


def add_matches_pattern_validation(validation_dict, field_dict, field_id):
   """
   The "has pattern to match" function checks to make sure this code should be called. 
   - Any assumptions made in this code about existing attributes/values should first be checked
   in the "has pattern to match" function before calling this.
   - If no pattern is explicitly provided, the default file path pattern is applied.
   """
   pattern = str()
   if field_attribute_value_exists(field_id=field_id, field_dict=field_dict, attribute_to_check=PATTERN_TO_MATCH_KEY):
      pattern = str(field_dict[field_id][PATTERN_TO_MATCH_KEY]).strip()
      
   else:
      pattern = "^gs://"
   
   validation = MatchesPatternValidation(pattern)
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_object_to_add=validation)


def add_category_value_validation(validation_dict, field_id, allowed_values_list):
   if allowed_values_list is None or len(allowed_values_list) < 1:
      print("Bad allowed values list")
   
   validation = InListValidation(allowed_values_list)
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_object_to_add=validation)



def create_validation_build_dict(fields_dict, fields_to_validate_list, primary_key):
   """
   Checks schema config JSON to determine what validation should be applied & applies it to the schema for validation
   """
   dynamic_validation_build_dict = {}
   add_validation(
      field_id=primary_key,
      validation_dict=dynamic_validation_build_dict,
      validation_object_to_add=IsDistinctValidation()
   )

   for field_id in fields_to_validate_list:
      if field_id not in fields_dict:
         continue

      if field_attribute_value_exists(field_id=field_id, field_dict=fields_dict, attribute_to_check=IS_UNIQUE_KEY):
         add_validation(
            field_id=field_id,
            validation_dict=dynamic_validation_build_dict,
            validation_object_to_add=IsDistinctValidation()
         )

      if null_value_invalid(field_id=field_id, fields_dict=fields_dict):
         add_validation(
            field_id=field_id,
            validation_dict=dynamic_validation_build_dict, 
            validation_object_to_add=null_validation
         )

      if has_valid_pattern_match_value(field_id=field_id, field_dict=fields_dict):
         add_matches_pattern_validation(
            field_id=field_id, 
            field_dict=fields_dict,
            validation_dict=dynamic_validation_build_dict
         )

      if has_category_value_validation(field_id=field_id, field_dict=fields_dict):
         add_category_value_validation(
            field_id=field_id,
            validation_dict=dynamic_validation_build_dict,
            allowed_values_list=fields_dict[field_id][ALLOWED_VALUES_KEY]
         )

      if only_int_values_allowed(field_id=field_id, field_dict=fields_dict):
         add_validation(
            field_id=field_id,
            validation_dict=dynamic_validation_build_dict, 
            validation_object_to_add=IsDtypeValidation(int)
         )
      elif check_field_type(field_id, fields_dict, expected_type=NUMERIC_FIELD_TYPE_VAL):
         add_validation(
            field_id = field_id,
            validation_dict=dynamic_validation_build_dict,
            validation_object_to_add=IsDtypeValidation(float)
         )

   return dynamic_validation_build_dict      


# MAIN VALIDATION CODE
def dynamically_validate_df(data_df, field_dict, fields_to_validate_list, primary_key):
   dynamic_validation_build_guide_dict = create_validation_build_dict(fields_dict=field_dict, fields_to_validate_list=fields_to_validate_list, primary_key=primary_key)

   validation_code = create_validation_code_from_logic(
      validation_build_guide_dict=dynamic_validation_build_guide_dict, 
      fields_to_validate=fields_to_validate_list
   )

   errors = validation_code.validate(data_df, columns=(validation_code.get_column_names()))

   return errors


def create_validation_code_from_logic(validation_build_guide_dict, fields_to_validate):
   columns_logic_list = []
   
   for column_id in list(fields_to_validate):
      if column_id not in list(validation_build_guide_dict.keys()):
         column = Column(name=column_id)
      
      else:
         validation_logic_list = validation_build_guide_dict[column_id]
         column = Column(name=column_id, validations=validation_logic_list)
         
      columns_logic_list.append(column)
   
   return Schema(columns_logic_list)