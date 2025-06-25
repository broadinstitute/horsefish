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

# Copy the project structure
COPY /src/ ./src/
COPY /config/ ./config/
COPY /scripts/ ./scripts/
COPY /tests/ ./tests/
COPY /docs/ ./docs/
COPY /requirements.txt ./

RUN pip install -r requirements.txt

# Set Python path to include src
ENV PYTHONPATH="/scripts/tdr/copy_from_tdr_to_gcs_hca/src:${PYTHONPATH}"

ENV PYTHONPATH "/scripts:${PYTHONPATH}"
CMD ["/bin/bash"]
