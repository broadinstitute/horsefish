# This script is used to build and push the general python docker image to the google container registry
# usage: ./update_gen_py_docker_image.sh <version_number>
set -e

VERSION="${1}"
BUILD_TAG="us-east4-docker.pkg.dev/dsp-fieldeng-dev/horsefish/general_python:${VERSION}"

echo "using build tag: ${BUILD_TAG}"

docker build . -t ${BUILD_TAG}
docker push ${BUILD_TAG}