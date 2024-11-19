FROM python:3.10.14-alpine

ENV HOME=/

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY * /scripts/bacterial_genomics/


# OS updates for security
RUN apk update
RUN apk upgrade
RUN apk add --no-cache bash

ENV PYTHONPATH "/scripts:${PYTHONPATH}"

CMD ["/bin/bash"]
