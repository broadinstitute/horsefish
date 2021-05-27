#!/bin/bash
set -e

# if num arguments != 2 || file does not exist || filename suffix != ".xlsx" || col number != integer
if [ "$#" -ne 2 ] || [ ! -f "$1" ] || grep -q xlsx "$1" || [[ ! $2 =~ ^[[:digit:]]+$ ]];
  then
    echo "Missing required arguments or incorrect argument format/type."
    echo "Usage: $0 [EXCEL_FILE] [COLUMN_NUMBER_TO_CHECK]"
    echo "EXCEL_FILE: path to input excel (.xlsx) file."
    echo "COLUMN_NUMBER_TO_CHECK: integer/number of column with gs://fc-XXXX file paths validate."
    exit 0
fi

# define variables for cmd parameters
excel_file=$1
check_col_num=$2
# get basename for excel file
excel_basename="$(basename "$excel_file" .xlsx)"

# convert xlsx to txt
echo "Converting xlsx to tsv."
ssconvert "$excel_file" "${excel_basename}.txt"
# substitute comma to tab | remove header | get col of crai paths > write tmp file
tr "," "\t" < "${excel_basename}.txt" | sed -e 1d | cut -f"$check_col_num" > "${excel_basename}.tmp"

echo "Checking if files exist."
# create tmp file with header to capture file status (True or False)
echo "file_exists" >> exists_file.tmp

# for each path in file of paths
while IFS="" read -r gs_path
    do
        if ! gsutil -q stat "$gs_path"
            then
                echo "False" >> exists_file.tmp
        else
                echo "True" >> exists_file.tmp
        fi
done < "${excel_basename}.tmp"

echo "Creating new excel file with additional column named: '${excel_basename}_with_exists.xlsx'."
# combine exists file + original txt > tmp file with additional column
paste -d "," "${excel_basename}.txt" exists_file.tmp > with_exists.tmp
# convert tmp to final excel
ssconvert with_exists.tmp "${excel_basename}_with_exists.xlsx"

# remove tmp files created
rm *.tmp