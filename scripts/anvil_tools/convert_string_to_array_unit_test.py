import batch_upsert_entities_standard

# TEST: Converting a string into an array that is compatible with a tsv data model upload
test_input_string = '"foo", "bar"'
expected_output = ['foo', 'bar']
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output

test_input_string = "'foo', 'bar'"
expected_output = ['foo', 'bar']
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output

test_input_string = ["foo", "bar"]
expected_output = ['foo', 'bar']
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output

test_input_string = ["foo",   "  bar"]
expected_output = ['foo', 'bar']
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output

test_input_string = ['foo', 'bar']
expected_output = ['foo', 'bar']
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output

test_input_string = "foo"
expected_output = ["foo"]
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output

test_input_string = 'foo'
expected_output = ["foo"]
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output

test_input_string = ["foo"]
expected_output = ["foo"]
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output

test_input_string = ['foo']
expected_output = ["foo"]
assert batch_upsert_entities_standard.convert_string_to_list(test_input_string) == expected_output
