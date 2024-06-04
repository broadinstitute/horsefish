FROM python:3.10.14-alpine

WORKDIR /app

COPY . .

RUN pip3 install --upgrade pip && pip3 install -r requirements.txt

ENTRYPOINT  ["python3", "-m", "ingest"  ]
