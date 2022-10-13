FROM python:3.8-slim-buster

# Tell gcloud to save state in /.config so it's easy to override as a mounted volume.
ENV HOME=/

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY add_service_alert.py /scripts/add_service_alert.py
COPY clear_service_alerts.py /scripts/clear_service_alerts.py
COPY remove_service_alert.py /scripts/remove_service_alert.py
COPY utils.py /scripts/utils.py

ENV PYTHONPATH "/scripts:${PYTHONPATH}"

CMD ["/bin/bash"]