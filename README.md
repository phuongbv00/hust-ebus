# HUST eBus

## Prerequisites

- [Python 3.12](https://www.python.org/)
- [uv](https://docs.astral.sh/uv/)
- [Docker](https://www.docker.com/)
- [NodeJS](https://nodejs.org/en/download)

## Setup

### Environment Variables

1. Clone `.env` file base on `.env.example`
2. Fill up `.env`

### Database

```shell
docker compose -f docker-compose.db.yaml up -d
```

### MinIO

```shell
docker compose -f docker-compose.s3.yaml up -d
```

### Spark Cluster

```shell
docker compose -f docker-compose.spark.yaml up -d --scale spark-worker=3
```

### Kafka

```shell
docker compose -f docker-compose.kafka.yaml up -d
```

### Debezium

```shell
docker compose -f docker-compose.debezium.yaml up -d
```

## Development

### Virtual Environment

Create venv (1st time)

```shell
uv venv
```

Activate venv

- Linux/MacOS

    ```shell
    source .venv/bin/activate
    ```

- Windows

    ```shell
    # In cmd.exe
    venv\Scripts\activate.bat
    
    # In PowerShell
    venv\Scripts\Activate.ps1
    ```

### Install Dependencies

```shell
uv sync
```

### Run Application

#### Setup PYTHONPATH

- Option 1: IDE
    - PyCharm:
        - Right-click on folder `app` → Mark Directory as → Sources Root
        - Right-click on folder `test` → Mark Directory as → Test Sources Root
- Option 2: Terminal
    - MacOS/Linux Terminal:
      ```shell
      export PYTHONPATH=app:test
      ```
    - Windows Powershell:
      ```shell
      $env:PYTHONPATH="app;test"
      ```

#### Run Batch Worker

```shell
python app/batch/main.py
```

#### Run Bootstrap Job

Bootstrap job setup database, initial data, topics, ...

MUST run before stream worker.

```shell
python test/bootstrap.py
```

#### Run Stream Worker

```shell
python app/stream/main.py
```

#### Run Serving Service

- Option 1: PyCharm
    - Right-click on `app/serving/main.py` → Run 'main'
- Option 2: Terminal
    ```shell
    python app/serving/main.py
    ```

#### Run UI

```shell
cd ui
npm install
npm run dev
```

Access to http://localhost:3000

### Evaluation

#### Evaluate UC01

```shell
python test/uc01.py
```

#### Evaluate UC02

```shell
python test/uc02.py
```

#### Evaluate UC03

```shell
python test/uc03.py
```