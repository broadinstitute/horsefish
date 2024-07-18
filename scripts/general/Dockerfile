FROM us.gcr.io/broad-dsp-gcr-public/base/python:3.12-alpine

RUN apk add --no-cache bash

COPY find_loop.py /scripts/find_loop.py

ENV PYTHONPATH "/scripts:${PYTHONPATH}"
CMD ["/bin/bash"]