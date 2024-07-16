import json
import sys

input_file = sys.argv[1]

# pull out basename of asset list, try to extract suffix, definitely extract txt suffix
asset_name = input_file.split("/")[-1].replace("_asset_cols.txt", "").replace(".txt", "")

def format_tables(filename):
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
        tables.append({ "name" : table_name, "columns" : columns })

    return tables, root_column, root_table


tables, root_column, root_table = format_tables(input_file)

asset_json = {
  "follow": [],
  "name": asset_name,
  "rootColumn": root_column,
  "rootTable": root_table,
  "tables": tables
}

print(json.dumps(asset_json, sort_keys=True, indent=4))