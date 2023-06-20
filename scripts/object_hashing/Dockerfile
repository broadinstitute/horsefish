FROM google/cloud-sdk:435.0.1

# Tell gcloud to save state in /.config so it's easy to override as a mounted volume.
ENV HOME=/

COPY create_object_md5.py /scripts/create_object_md5.py

RUN pip3 install google-cloud-storage
RUN pip3 install google-api-python-client

ENV PYTHONPATH "/scripts:${PYTHONPATH}"
CMD ["/bin/bash"]