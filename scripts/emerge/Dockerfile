FROM python:3.8-slim-buster

# Tell gcloud to save state in /.config so it's easy to override as a mounted volume.
ENV HOME=/

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ingest_to_tdr.py /scripts/emerge/ingest_to_tdr.py

ENV PYTHONPATH "/scripts:${PYTHONPATH}"

CMD ["/bin/bash"]