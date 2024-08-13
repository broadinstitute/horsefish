FROM us.gcr.io/broad-dsp-gcr-public/base/python:3.12-alpine

ENV PATH /google-cloud-sdk/bin:$PATH
RUN if [ `uname -m` = 'x86_64' ]; then echo -n "x86_64" > /tmp/arch; else echo -n "arm" > /tmp/arch; fi;
RUN ARCH=`cat /tmp/arch` && apk --no-cache upgrade && apk --no-cache add \
        bash    \
        curl \
        python3 \
        py3-crcmod \
        py3-openssl \
        bash \
        libc6-compat \
        openssh-client \
        git \
        gnupg \
    && curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz && \
    tar xzf google-cloud-cli-linux-x86_64.tar.gz && \
    rm google-cloud-cli-linux-x86_64.tar.gz && \
    gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment docker_image_alpine && \
    gcloud --version
RUN git config --system credential.'https://source.developers.google.com'.helper gcloud.sh
VOLUME ["/root/.config"]

WORKDIR /scripts/tdr/copy_from_tdr_to_gcs_hca

# copy the contents of /scripts/tdr/copy_from_tdr_to_gcs_hca to the WORKDIR
COPY * .

RUN pip install -r requirements.txt

ENV PYTHONPATH "/scripts:${PYTHONPATH}"
CMD ["/bin/bash"]

# builds with GitHub Action "Main Validation and Release" ../.github/workflows/build-and-push_docker_main.yaml and ../.github/workflows/build-and-push_docker_dev.yaml
# tags = us-east4-docker.pkg.dev/$GCP_PROJECT_ID/$GCP_REPOSITORY/copy_from_tdr_to_gcs_hca:$GITHUB_SHA, us-east4-docker.pkg.dev/$GCP_PROJECT_ID/$GCP_REPOSITORY/copy_from_tdr_to_gcs_hca:latest

# To manually build and run locally
# docker build -t us-east4-docker.pkg.dev/dsp-fieldeng-dev/horsefish/copy_from_tdr_to_gcs_hca:<new_version> .
# docker run --rm -it us-east4-docker.pkg.dev/dsp-fieldeng-dev/horsefish/copy_from_tdr_to_gcs_hca:<new_version>

# to build and push to Artifact Registry
# make sure you are logged in to gcloud and that application default credentials are set
# gcloud auth login
# gcloud config set project dsp-fieldeng-dev
# gcloud auth application-default login
# set the <new_version> before building and pushing
# docker push us-east4-docker.pkg.dev/dsp-fieldeng-dev/horsefish/copy_from_tdr_to_gcs_hca:<new_version>