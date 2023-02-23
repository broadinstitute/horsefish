#!/bin/bash
set -e

# DEVELOPER: update this field anytime you make a new docker image
VERSION="2.0"
BUILD_TAG="broadinstitute/horsefish:v${VERSION}"

echo "using build tag: ${BUILD_TAG}"

if docker manifest inspect $BUILD_TAG > /dev/null ;
then
    echo "Docker image already exists. Do you want to overwrite this tag?."
    read -p "Press [Enter] to overwrite or [Ctrl+C] to exit and change the version number."
fi

echo "Building and pushing new image ${BUILD_TAG}"

docker build . -t ${BUILD_TAG}
docker push ${BUILD_TAG}
