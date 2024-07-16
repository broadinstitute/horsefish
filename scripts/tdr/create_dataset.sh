TOKEN=$(gcloud auth print-access-token)

curl -sL -X POST "https://data.terra.bio/api/repository/v1/datasets" -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer ${TOKEN}" --data @${1} | jq