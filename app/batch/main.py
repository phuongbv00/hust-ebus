import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware

from batch.core import JobOrchestrator, JobExecution

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

job_orchestrator = JobOrchestrator()


class JobExecutionResponse(BaseModel):
    id: str
    name: str
    status: int
    error: str | None = None
    execution_time: float | None = None

    @classmethod
    def from_(cls, job_execution: JobExecution):
        return cls(
            id=job_execution.id,
            name=job_execution.name,
            status=job_execution.status,
            error=str(job_execution.error) if job_execution.error else None,
            execution_time=job_execution.execution_time,
        )


def smart_cast(value: str):
    """Try to infer and cast a query string value to an appropriate type."""
    val = value.strip()

    if val.lower() == "true":
        return True
    if val.lower() == "false":
        return False
    if val.isdigit():
        return int(val)
    try:
        return float(val)
    except ValueError:
        pass
    if "," in val:
        return [smart_cast(v) for v in val.split(",")]
    return val  # fallback to string


@app.get("/job/{name}")
def launch_job(name: str, request: Request):
    raw_kwargs = dict(request.query_params)
    kwargs = {k: smart_cast(v) for k, v in raw_kwargs.items()}
    return JobExecutionResponse.from_(job_orchestrator.launch(name, **kwargs))


@app.get("/job/{name}/executions")
def get_job_executions(name: str):
    return [
        JobExecutionResponse.from_(execution)
        for execution in job_orchestrator.get_executions(name)
    ]


if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv('BATCH_PORT')), reload=True)
