FROM python:3.11-slim

WORKDIR /app

RUN pip install confluent-kafka==2.4.0

COPY app.py .

CMD ["python", "app.py"]
