set -e

# DEVELOPER: update this field anytime you make a new docker image
VERSION="2.0"
BUILD_TAG="broadinstitute/horsefish:v${VERSION}"

echo "using build tag: ${BUILD_TAG}"

docker build . -t ${BUILD_TAG}
docker push ${BUILD_TAG}