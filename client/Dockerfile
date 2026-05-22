FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app/client

COPY client/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY client /app/client

CMD ["python", "main.py"]
