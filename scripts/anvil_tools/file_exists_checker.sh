#!/bin/bash
set -e

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
# create tmp file for col of exists or not exists values
echo "file_exists" >> exists_file.tmp

# for each path in file of paths
while IFS="" read -r gs_path
    do
        if ! gsutil -q stat "$gs_path"
            then
                echo "File does not exist." >> exists_file.tmp
        else
                echo "File exists." >> exists_file.tmp
        fi
done < "${excel_basename}.tmp"

echo "Creating new excel file with additional column - 'file_exists'."
paste -d "," "${excel_basename}.txt" exists_file.tmp > with_exists.tmp
ssconvert with_exists.tmp "${excel_basename}_with_exists.xlsx"

# remove tmp files created
rm *.tmp