FROM python:3.8-slim-buster

# # don't run as root
# RUN groupadd -g 999 appuser && \
#     useradd -r -u 999 -g appuser appuser
# USER appuser

# Tell gcloud to save state in /.config so it's easy to override as a mounted volume.
ENV HOME=/

# install gcloud
RUN apt-get update && apt-get install -y apt-transport-https gnupg curl lsb-release

RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
  echo "cloud SDK repo: $CLOUD_SDK_REPO" && \
  echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
  apt-get update -y && apt-get install google-cloud-sdk -y

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py /scripts/

ENV PYTHONPATH "/scripts:${PYTHONPATH}"

CMD ["/bin/bash"]
