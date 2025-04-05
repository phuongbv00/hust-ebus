# HUST Bus Stop

### Prerequisites

- Python 3.12
- Docker

### Environment Variables

1. Clone `.env` file base on `.env.example`
2. Fill up `.env`

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

### Spark Cluster

```shell
docker compose -f docker-compose.spark.yaml up -d --scale spark-worker=3
```