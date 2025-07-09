#!/bin/bash


# Check if a filename is provided as an argument
if [ $# -ne 1 ]; then
    echo "Usage: $0 <filename>"
    exit 1
fi

# Check if the file exists
if [ ! -f "$1" ]; then
    echo "File $1 not found"
    exit 1
fi

# Add a newline character at the end of the file
echo >> "$1"

# Read the file line by line
while IFS= read -r snapshot || [ -n "$snapshot" ]; do
    echo $snapshot
    # Make curl request to the API with the current snapshot
    response=$(curl -X 'GET' "https://data.terra.bio/api/repository/v1/snapshots/$snapshot/files?offset=0&limit=10000" -H 'accept: application/json' -H "Authorization: Bearer $(gcloud auth print-access-token)")

    echo "$response" > "response_$snapshot.json"
    jq '.[].fileDetail.accessUrl' "response_$snapshot.json" >> list_of_filepaths.txt
done < "$1"

#Replace gs:// with https://storage.cloud.google.com/
sed 's|gs://|https://storage.cloud.google.com/|g' list_of_filepaths.txt > tmp_file.txt
mv tmp_file.txt list_of_filepaths.txt

# Read the list of files from list_of_filepaths.txt and copy them using AzCopy in parallel
cat list_of_filepaths.txt | xargs -P 5 -I{} azcopy copy "{}" "<INSERT STORAGE SAS URL>"