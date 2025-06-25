# Use uma imagem base oficial do Python
FROM python:3.11-slim

# Define o diretório de trabalho no contêiner
WORKDIR /app

# Copia o arquivo de dependências para o diretório de trabalho
COPY requirements.txt .

# Instala os pacotes especificados em requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código da aplicação
COPY dashboard.py .

# Expõe a porta 8501 para o mundo fora deste contêiner
EXPOSE 8501

# Comando para executar o dashboard quando o contêiner for iniciado
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]