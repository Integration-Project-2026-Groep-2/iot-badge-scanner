FROM python:3.13-slim


ENV PYTHONUNBUFFERED=1 \
	PYTHONPATH=/app

RUN apt-get update && apt-get install -y libxml2  libxslt1.1 && rm -rf /var/lib/apt/lists/*

WORKDIR /app/server

COPY server/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY server /app/server

CMD ["python", "main.py"]
