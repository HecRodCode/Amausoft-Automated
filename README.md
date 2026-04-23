# Emausoft-Automated

## About the Project

This is a study project to learn Data Architecture. We use Python and uv to build an automatic system that gets data from APIs and processes it.

---

## How it Works

The project uses a Master/Worker setup with Docker:

- **Master**: Manages the tasks and the flow.
- **Worker**: Does the heavy work (ETL).

---

## How Run
1. Clone repo = https://github.com/HecRodCode/Amausoft-Automated.git
2. And execute the next commands:
```bash
# 1. Sync dependencies
uv sync

# 2. Run FastAPI
uv run uvicorn main:app --reload
```
---

## Folder Structure
- `src/`: Everything for the APIs (Routes, Services, Utils).

- `scripts/`: All the ETL functions (Cleaning and moving data).

- `dags/`: The automation files that run everything.

- `deployments/`: Docker files for the Master and Worker.

---

## Tech Stack

- `FastAPI`: To build and manage API logic.
- `Pandas`: For data manipulation and cleaning.
- `HTTPX`: To make requests to public APIs.
- `Apache Airflow`: To run and schedule our DAGs.
- `Docker`: To run Master and Worker instances.
- `uv`: For fast Python package management.

---

## Team Members

- **Héctor Ríos**
- **Camilo Guengue**
