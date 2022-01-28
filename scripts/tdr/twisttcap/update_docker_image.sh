
BUILD_TAG="broadinstitute/horsefish:twisttcap_scripts"

docker build . -t ${BUILD_TAG}
docker push ${BUILD_TAG}