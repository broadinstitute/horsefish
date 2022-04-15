
TOKEN=$(gcloud auth print-access-token)

if (( $# < 2 )); then
  echo "Usage: $0 DATASET_ID PATH_TO_CONFIGJSON"
  exit 1
fi

DATASET_ID=$1
CONFIG_JSON=$2

STEWARDS=$(cat $CONFIG_JSON | jq '.shareWith.steward|.[]')
CUSTODIANS=$(cat $CONFIG_JSON | jq '.shareWith.custodian|.[]')

for STEWARD_EMAIL in $STEWARDS
do
    echo "assigning steward policy to $STEWARD_EMAIL on dataset $DATASET_ID"
    curl -sL -X POST "https://data.terra.bio/api/repository/v1/datasets/${DATASET_ID}/policies/steward/members" -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer ${TOKEN}" -d "{ \"email\": \"${STEWARD_EMAIL}\"}" | jq
done

for CUSTODIAN_EMAIL in $CUSTODIANS
do
    echo "assigning custodian policy to $CUSTODIAN_EMAIL on dataset $DATASET_ID"
    curl -sL -X POST "https://data.terra.bio/api/repository/v1/datasets/${DATASET_ID}/policies/custodian/members" -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer ${TOKEN}" -d "{ \"email\": \"${CUSTODIAN_EMAIL}\"}" | jq
done
