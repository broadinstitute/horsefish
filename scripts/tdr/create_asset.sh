TOKEN=$(gcloud auth print-access-token)

DATASET_ID=$1
ASSET_CONFIG=$2

echo "generating asset json based off of ${ASSET_CONFIG}"
ASSET_JSON="asset.json"
python create_asset_json.py ${ASSET_CONFIG} | tee ${ASSET_JSON}

curl -sL -X POST "https://data.terra.bio/api/repository/v1/datasets/${DATASET_ID}/assets" -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer ${TOKEN}" --data @${ASSET_JSON} | jq

rm ${ASSET_JSON}