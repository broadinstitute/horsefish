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

REQUIRED_HEADERS = set(['name', 'datatype', 'array_of'])

def generate_columns(filename):
    columns = []

    with open(filename, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        # check for the proper headers
        column_names = set(csvReader.fieldnames)
        if len(REQUIRED_HEADERS - column_names) > 0:
            missing_headers = list(REQUIRED_HEADERS - column_names)
            raise Exception(f"The following required headers are missing from your csv: {missing_headers}.")

        # Convert each row into a dictionary
        # and add it to data
        for row in csvReader:
            # input file should be a csv with fields name, datatype, array_of; array_of can be blank, which defaults to false
            # some basic type checking on datatypes.
            if row["datatype"].lower() not in ACCEPTED_DATATYPES:
                bad_value = row["datatype"]
                raise TypeError(f"Unrecognized datatype: {bad_value}. Accepted datatypes are: {ACCEPTED_DATATYPES}")
            columns.append({"name": row["name"], "datatype": row["datatype"].lower(), "array_of": bool(row["array_of"])})
    return columns


def get_schema_tables(table_name_dict, project_name):
    tables_json_list = []

    for table_name, primary_key in table_name_dict.items():
        tables_json_list.append(
            {
                "name": table_name,
                "columns": generate_columns(f"{project_name}/schema_configs/{table_name}.csv"),
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

REQUIRED_CONFIG_FIELDS = ["project_name", "dataset_name", "tables_with_pks", "billing_profile", "description"]

def get_project_info(project_config):
    with open(project_config, "r") as infile:
        project_info = json.load(infile)

    for required_field in REQUIRED_CONFIG_FIELDS:
        assert(required_field in project_info.keys())

    return project_info

def get_schema_json(project_config):
    project_info = get_project_info(project_config)

    schema = {
        "tables": get_schema_tables(project_info["tables_with_pks"], project_info["project_name"])
    }

    if "assets" in project_info:
        schema["assets"] = get_assets(project_info["assets"], project_info["project_name"])

    if "enableSecureMonitoring" in project_info:
        schema["enableSecureMonitoring"] = project_info["enableSecureMonitoring"]

    full_json = {
        "cloudPlatform": "gcp",
        "defaultProfileId": project_info["billing_profile"],
        "description": project_info["description"],
        "name": project_info["dataset_name"],
        "schema": schema
    }

    if "region" in project_info:
        full_json["region"] = project_info["region"]

    print(json.dumps(full_json, sort_keys=True, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--project_config', '-c', required=True, help="path to file defining config")

    args = parser.parse_args()

    get_schema_json(args.project_config)
