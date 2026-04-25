FROM apache/airflow:2.10.2-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY --chown=airflow:0 . .