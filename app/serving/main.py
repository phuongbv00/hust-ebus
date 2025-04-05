import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()
if __name__ == '__main__':
    uvicorn.run("main:app", host="127.0.0.1", port=int(os.getenv('SERVING_PORT')), reload=True)
