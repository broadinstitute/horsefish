python create_schema_json.py -c gp-tim_config.json > payload.json

TOKEN=$(gcloud auth print-access-token)

curl -sL -X POST "https://data.terra.bio/api/repository/v1/datasets" -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer ${TOKEN}" --data @payload.json | jq

# rm payload.json