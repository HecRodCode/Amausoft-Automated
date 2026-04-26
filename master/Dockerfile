FROM apache/airflow:2.10.2-python3.11

USER root

# 1. System dependency installation
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /opt/airflow

# Copy Config File
COPY --chown=airflow:0 pyproject.toml .

# Install libreries
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    httpx \
    fastapi \
    uvicorn \
    psycopg2-binary \
    asyncio \
    kaggle

COPY --chown=airflow:0 . .

# PYTHONPATH Config
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src:/opt/airflow"
EXPOSE 8000