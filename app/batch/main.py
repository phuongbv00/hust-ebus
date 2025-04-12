import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from pydantic import BaseModel

from batch.core import JobOrchestrator, JobExecution

load_dotenv()

app = FastAPI()

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


@app.get("/job/{name}")
def launch_job(name: str, request: Request):
    kwargs = dict(request.query_params)
    return JobExecutionResponse.from_(job_orchestrator.launch(name, **kwargs))


@app.get("/job/{name}/executions")
def get_job_executions(name: str):
    return [
        JobExecutionResponse.from_(execution)
        for execution in job_orchestrator.get_executions(name)
    ]


if __name__ == '__main__':
    uvicorn.run("main:app", host="127.0.0.1", port=int(os.getenv('BATCH_PORT')), reload=True)
