set -e

VERSION=$(cat export_pipeline_outputs_to_tdr.py | grep docker_version | cut -d'"' -f2)
BUILD_TAG="broadinstitute/horsefish:tdr_import_v${VERSION}"

echo "using build tag: ${BUILD_TAG}"

docker build . -t ${BUILD_TAG}
docker push ${BUILD_TAG}