import os
import threading
from contextlib import asynccontextmanager

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

from stream.pipeline import uc02

load_dotenv()

# Register background tasks
bg_tasks = [uc02.run]


@asynccontextmanager
async def lifespan(app: FastAPI):
    for task in bg_tasks:
        thread = threading.Thread(target=task, daemon=True)
        thread.start()
    yield


app = FastAPI(lifespan=lifespan)

if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv('STREAM_PORT')), reload=True)
