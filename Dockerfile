FROM python:3.11-slim-bookworm

LABEL maintainer="Seu Nome <seu@email.com>"

WORKDIR /app

# Atualiza pacotes, instala build essentials temporariamente e remove depois
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copia e instala dependências Python
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Remove gcc para minimizar vulnerabilidades
RUN apt-get purge -y gcc && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Copia o restante da aplicação
COPY . .

# Define porta e comando padrão
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
