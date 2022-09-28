FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:404.0.0-slim

# Tell gcloud to save state in /.config so it's easy to override as a mounted volume.
ENV HOME=/

COPY requirements.txt .
RUN apt-get install python3-pip
RUN pip3 install -r requirements.txt

COPY setup_new_wfl_module.py /scripts/setup_new_wfl_module.py

ENV PYTHONPATH "/scripts:${PYTHONPATH}"

CMD ["/bin/bash"]
