# Use the official Python slim image
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
VOLUME ["/app"]

RUN pip install --upgrade pip \
 && pip install --no-cache-dir \
      google-api-python-client \
      google-auth \
      schedule \
      paho-mqtt \
      ha-mqtt-discoverable

CMD ["python", "PCO-Sheets.py"]
