set -e

VERSION=$(cat setup_new_wfl_workload.py | grep docker_version | cut -d'"' -f2)
BUILD_TAG="broadinstitute/horsefish:twisttcap_wfl_setup_v${VERSION}"

echo "using build tag: ${BUILD_TAG}"

docker build . -t ${BUILD_TAG}
docker push ${BUILD_TAG}