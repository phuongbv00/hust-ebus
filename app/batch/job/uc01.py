import time

from batch.core import Job


class JobUC01(Job):
    def run(self, *args, **kwargs):
        print("JobUC01 starting...")
        time.sleep(30)
        print("JobUC01 done")
        pass
