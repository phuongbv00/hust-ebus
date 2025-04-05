import importlib
import os
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any


class Job(ABC):
    def __init__(self):
        self.name = self.__class__.__name__.lower()

    @abstractmethod
    def run(self, *args, **kwargs):
        pass


class JobNotFoundError(Exception):
    pass


def _load_jobs_from_env() -> dict[str, type[Job]]:
    enabled_job_names = os.getenv("BATCH_ENABLED_JOBS", "").split(",")
    jobs = {}

    for job_name in map(str.strip, enabled_job_names):
        if not job_name:
            continue

        env_var = f"BATCH_JOB_{job_name}_PATH"
        full_class_path = os.getenv(env_var)

        if not full_class_path:
            raise JobNotFoundError(f"{job_name}: no class path defined in {env_var}")

        try:
            module_path, class_name = full_class_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            job_class = getattr(module, class_name)
            if not issubclass(job_class, Job):
                raise JobNotFoundError(f"{job_name}: {class_name} is not a subclass of Job")
            jobs[job_name] = job_class
        except (ImportError, AttributeError) as e:
            raise JobNotFoundError(f"{job_name}: failed to import {full_class_path}: {e}")

    return jobs


class JobExecution:
    def __init__(self, name: str):
        self.id = str(uuid.uuid4())
        self.name = name
        self.status: int = 0
        self.error: Exception | None = None
        self.execution_time: float | None = None
        self.result: Any = None
        self.thread: threading.Thread | None = None


class JobOrchestrator:
    def __init__(self):
        self.jobs = _load_jobs_from_env()
        self.executions: list[JobExecution] = []

    def get(self, name: str):
        job_class = self.jobs.get(name)
        if job_class:
            return job_class()
        raise JobNotFoundError(name)

    def launch(self, name: str, *args, **kwargs) -> JobExecution:
        job_class = self.jobs.get(name)
        if not job_class:
            raise JobNotFoundError(name)

        job = job_class()
        execution = JobExecution(name)

        def run_job():
            start = time.time()
            try:
                execution.result = job.run(*args, **kwargs)
                execution.status = 1
            except Exception as e:
                execution.status = -1
                execution.error = e
            finally:
                execution.execution_time = time.time() - start

        thread = threading.Thread(target=run_job)
        thread.start()

        execution.thread = thread
        self.executions.append(execution)

        return execution

    def get_executions(self, name: str | None = None) -> list[JobExecution]:
        if name:
            return [exe for exe in self.executions if exe.name == name]
        return self.executions
