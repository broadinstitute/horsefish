set -e

VERSION=$(cat ingest.py | grep docker_version | cut -d'"' -f2)
BUILD_TAG="broadinstitute/horsefish:bacterialingest_v${VERSION}"

echo "using build tag: ${BUILD_TAG}"

docker build . -t ${BUILD_TAG}
docker push ${BUILD_TAG}
