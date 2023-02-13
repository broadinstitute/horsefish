"""Validate dataframes with validators."""
from pandas_schema import*
from pandas_schema.validation import*
import re

# validators
# column values cannot be null
null_validation = CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')

      
def field_exists_in_df(field_id, df):
    if field_id in list(df.keys()):
        return True
    return False


# unclear if this is necessary
def required_field(field_id, field_dict):
   if field_id not in list(field_dict.keys()):
      return False
   field_id_keys = list(field_dict[field_id].keys())

   if get_optional_field_key() not in field_id_keys:
      return True

   if field_attribute_is_true(attribute_to_check=get_optional_field_key(), field_id=field_id, field_dict=field_dict):
      return False

   return True


def field_attribute_is_true(field_id, field_dict, attribute_to_check):
   # handle null defaults to "true" on a field-by-field basis outside this function
   if attribute_to_check not in list(field_dict[field_id].keys()):
      return False

   if field_dict[field_id][attribute_to_check] == "True":
      return True
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
      return False

   field_type = field_dict[field_id][type_key]

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

   return dynamic_validation_build_dict      


def null_value_invalid(fields_dict, field_id):
   field_id_keys = list(fields_dict[field_id].keys())
   value_required_key = get_value_required_key()

   if value_required_key not in field_id_keys:
      return False

   elif field_attribute_is_true(attribute_to_check=value_required_key, field_id=field_id, field_dict=fields_dict):
      return True

   return False


def add_validation(validation_dict, field_id, validation_object_to_add):
   if field_id not in list(validation_dict.keys()):
      validation_dict[field_id] = []

   validation_dict[field_id].append(validation_object_to_add)


def add_null_validation(validation_dict, field_id):
   validation = null_validation
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_object_to_add=validation)


def add_matches_pattern_validation(validation_dict, field_dict, field_id):
   """
   TODO: 
   this is somewhat redundant with the has_valid_pattern_match_logic function;
   should look into whether there's a way to consolidate this logic more
   """
   pattern = str()
   if field_attribute_value_exists(
      field_id=field_id, 
      field_dict=field_dict, 
      attribute_to_check=get_field_pattern_to_match_key()
   ):
      pattern = str(field_dict[field_id][get_field_pattern_to_match_key()]).strip()

   # TODO: allow JSON to specify custom default patterns for file_path fields
   if (pattern is None or str(pattern).strip() == "")\
      and is_file_path_field_type(field_id=field_id,field_dict=field_dict):
      pattern = "^gs://"
   
   validation = MatchesPatternValidation(pattern)
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_object_to_add=validation)


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
   
   validation = InListValidation(allowed_values_list)
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_object_to_add=validation)


def add_integer_only_validation(validation_dict, field_id):
   validation = IsDtypeValidation(int)
   add_validation(validation_dict=validation_dict, field_id=field_id, validation_object_to_add=validation)



# MAIN VALIDATION CODE
def dynamically_validate_df(data_df, field_dict, fields_to_validate_list):
   dynamic_validation_build_guide_dict = create_validation_build_dict(fields_dict=field_dict, fields_to_validate_list=fields_to_validate_list)
   
   validation_code = create_validation_code_from_logic(
      validation_build_guide_dict=dynamic_validation_build_guide_dict, 
      fields_to_validate=fields_to_validate_list
   )

   errors = validation_code.validate(data_df, columns=(validation_code.get_column_names()))
   for error in errors:
      print(error)

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