import os
import time

from dotenv import load_dotenv
from urllib3 import request
import urllib.parse

load_dotenv()


def test_job(job_name: str, **kwargs):
    params = urllib.parse.urlencode(kwargs)
    res = request("get", f"http://localhost:{os.getenv('BATCH_PORT')}/job/{job_name}?{params}")
    res_json = res.json()
    job_id = res_json["id"]
    print(res_json)
    end = 0
    while end != 1:
        time.sleep(3)
        res = request("get", f"http://localhost:{os.getenv('BATCH_PORT')}/job/{job_name}/executions")
        res_json = res.json()
        for job_exec in res_json:
            if job_exec["id"] == job_id:
                print(job_exec)
                if job_exec["status"] != 0:
                    end = 1
