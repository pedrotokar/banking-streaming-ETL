FROM python:3.9-slim

WORKDIR /app

# Instalar Java (necessário para Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

# Instalar PySpark e outras dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte
COPY consumer.py .

CMD ["python", "consumer.py"] 