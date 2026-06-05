FROM python:3.11-slim

WORKDIR /app

RUN pip install confluent-kafka==2.4.0 psycopg2-binary==2.9.9

COPY app.py .

CMD ["python", "app.py"]
