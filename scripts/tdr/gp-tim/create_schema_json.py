import argparse
import json
import csv


# the following is sourced from https://docs.google.com/document/d/161j6YLyEXiGH8ujD-PnHZe0jdQ_p6C9OONpglMq_ULw/edit#heading=h.kkoujqgz2zd6
ACCEPTED_DATATYPES = ['boolean',
                      'bytes',
                      'date',
                      'datetime',
                      'time',
                      'timestamp',
                      'float',
                      'float64',
                      'integer',
                      'int64',
                      'numeric',
                      'string',
                      'text',
                      'fileref',
                      'dirref']

REQUIRED_HEADERS = set(['table_name','field_name','datatype','array_of', 'relationships'])

def extract_table_data(filename):
    columns = []

    with open(filename, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        # check for the proper headers
        column_names = set(csvReader.fieldnames)
        if len(REQUIRED_HEADERS - column_names) > 0:
            missing_headers = list(REQUIRED_HEADERS - column_names)
            raise Exception(f"The following required headers are missing from your csv: {missing_headers}.")

        tables = []
        relationships = {}

        # Convert each row into a dictionary
        # and add it to data
        for row in csvReader:
            # input file should be a csv with fields table_name, field_name, datatype, array_of, relationships;
            # array_of can be blank, which defaults to false
            # some basic type checking on datatypes.
            if row["datatype"].lower() not in ACCEPTED_DATATYPES:
                bad_value = row["datatype"]
                raise TypeError(f"Unrecognized datatype: {bad_value}. Accepted datatypes are: {ACCEPTED_DATATYPES}")

            if row["table_name"] not in tables:
                tables.append(row["table_name"])

            if row["relationships"]:
                from_value = f'{row["table_name"]}.{row["field_name"]}'
                relationships[from_value] = row["relationships"]

            if row["array_of"] in ["True", "true", "TRUE"]:
                array_of = True
            else:
                array_of = False

            columns.append({
                "table_name": row["table_name"],
                "field_name": row["field_name"],
                "datatype": row["datatype"].lower(),
                "array_of": array_of
                })

    return tables, columns, relationships


def generate_columns(table_name, all_columns):
    columns = []
    for row in all_columns:
        if row["table_name"] == table_name:
            columns.append({"name": row["field_name"], "datatype": row["datatype"].lower(), "array_of": row["array_of"]})
    return columns


def format_relationships(relationships_dict):
    relationships = []

    for from_value, to_value in relationships_dict.items():
        relationships.append({
            "name": f"{from_value} to {to_value}",
            "from": {
                "table": from_value.split(".")[0],
                "column": from_value.split(".")[1]
            },
            "to":  {
                "table": to_value.split(".")[0],
                "column": to_value.split(".")[1]
            },
        })

    return relationships


def get_schema_tables(table_names, columns):
    tables_json_list = []

    for table_name in table_names:
        primary_key = f"{table_name}_id"
        tables_json_list.append(
            {
                "name": table_name,
                "columns": generate_columns(table_name, columns),
                "partitionMode": "none",
                "primaryKey": [
                    primary_key
                ],
                "rowCount": 0
            }
        )

    return tables_json_list

def get_assets(asset_name_list, project_name):
    assets_json_list = []

    for asset_name in asset_name_list:
        filename = f"{project_name}/asset_configs/{asset_name}_asset_cols.txt"

        tables = []
        table_info_dict = {}

        root_column = None
        root_table = None

        with open(filename, "r") as f:
            for line in f:
                line = line.strip()
                if "*" in line:
                    root_field = True
                    line = line.replace("*", "")
                else:
                    root_field = False

                table_name = line.split('.')[0]
                colname = line.split('.')[1]
                colname_clean = colname.replace("-","_")

                table_info_dict[table_name] = table_info_dict.get(table_name, []) + [colname_clean]
                if root_field:
                    root_column = colname_clean
                    root_table = table_name

        for table_name, columns in table_info_dict.items():
            tables.append({"name": table_name, "columns": columns})

        assets_json_list.append({
            "follow": [],
            "name": asset_name,
            "rootColumn": root_column,
            "rootTable": root_table,
            "tables": tables
            })

    return assets_json_list

REQUIRED_CONFIG_FIELDS = ["project_name", "dataset_name", "data_model_file", "billing_profile", "description"]

def get_project_info(project_config):
    with open(project_config, "r") as infile:
        project_info = json.load(infile)

    for required_field in REQUIRED_CONFIG_FIELDS:
        assert(required_field in project_info.keys())

    return project_info


def get_schema_json(project_config):
    project_info = get_project_info(project_config)

    # tables is a list
    table_names, columns, relationships = extract_table_data(project_info["data_model_file"])

    schema = {
        "tables": get_schema_tables(table_names, columns),
        "relationships": format_relationships(relationships)
    }

    if "assets" in project_info:
        schema["assets"] = get_assets(project_info["assets"], project_info["project_name"])

    full_json = {
        "cloudPlatform": "gcp",
        "defaultProfileId": project_info["billing_profile"],
        "description": project_info["description"],
        "name": project_info["dataset_name"],
        "schema": schema
    }


    if "enableSecureMonitoring" in project_info:
        full_json["enableSecureMonitoring"] = project_info["enableSecureMonitoring"]


    if "region" in project_info:
        full_json["region"] = project_info["region"]

    print(json.dumps(full_json, sort_keys=True, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--project_config', '-c', required=True, help="path to file defining config")

    args = parser.parse_args()

    get_schema_json(args.project_config)
