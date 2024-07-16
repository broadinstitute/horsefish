
TOKEN=$(gcloud auth print-access-token)

if (( $# < 3 )); then
  echo "Usage: $0 DATASET_ID NEW_MEMBER_EMAIL ROLE"
  echo "ROLE must be one of { steward custodian snapshot_creator }"
  exit 1
fi

DATASET_ID=$1
NEW_MEMBER_EMAIL=$2
ROLE=$3

curl -sL -X POST "https://data.terra.bio/api/repository/v1/datasets/${DATASET_ID}/policies/${ROLE}/members" -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer ${TOKEN}" -d "{ \"email\": \"${NEW_MEMBER_EMAIL}\"}" | jq

# curl -sL -X POST "https://data.terra.bio/api/repository/v1/datasets" -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer ${TOKEN}" --data @${1} | jq