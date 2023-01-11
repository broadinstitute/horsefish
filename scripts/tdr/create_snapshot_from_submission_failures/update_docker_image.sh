set -e

VERSION=$(cat create_snapshot_from_submission_failures.py | grep docker_version | cut -d'"' -f2)
BUILD_TAG="broadinstitute/horsefish:tdr_failures_v${VERSION}"

echo "using build tag: ${BUILD_TAG}"

docker build . -t ${BUILD_TAG}
docker push ${BUILD_TAG}