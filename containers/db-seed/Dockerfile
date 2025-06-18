FROM python:3.11-slim

# Instala dependências mínimas
RUN pip install --no-cache-dir psycopg2-binary numpy

WORKDIR /app
COPY historic_data_mock.py .

# O entrypoint é simplesmente rodar o script
ENTRYPOINT ["python", "historic_data_mock.py"]
