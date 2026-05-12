FROM python:3.13-slim


# NOTE(nasr): geen idee in hoe nuttig dit is. heeft te maken met het tonen van de docker logs
# belangrijk zegt men
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y libxml2  libxslt1.1 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
