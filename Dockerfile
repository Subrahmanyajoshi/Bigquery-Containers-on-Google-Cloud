FROM python:3.9.10

COPY dockerize /opt/
RUN pip install -r requirements.txt
ENV GOOGLE_APPLICATION_CREDENTIALS /opt/key.json

WORKDIR /opt/
ENTRYPOINT ["python3", "bigquery_pusher.py"]
CMD []
