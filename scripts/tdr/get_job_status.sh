
TOKEN=$(gcloud auth print-access-token)

curl -sL -X GET "https://data.terra.bio/api/repository/v1/jobs/${1}" -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer ${TOKEN}" | jq