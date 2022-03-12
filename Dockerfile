FROM python:3.9.10

COPY dockerize /opt/
WORKDIR /opt/

RUN pip install -r requirements.txt
ENV GOOGLE_APPLICATION_CREDENTIALS key.json

ENTRYPOINT ["python3", "bigquery_pusher.py"]
CMD []
