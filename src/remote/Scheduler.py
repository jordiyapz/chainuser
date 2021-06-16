import sqlite3
from src.services import Jobs
import ray

from src.util import StatusEnum


@ray.remote
class Scheduler:
    def __init__(self, db_path):
        self._conn = sqlite3.connect(db_path)
        self.job_service = Jobs(self._conn)
        self.idle_jobs = self.job_service.get_by_status()

    def get_next(self):
        return next(self.idle_jobs)

    def set_job(self, screen_name, status: StatusEnum):
        job_id = self.job_service.get_id(screen_name)
        if job_id is None:
            raise ValueError(f'{screen_name} not exists')

        self.job_service.set_job(job_id, status)
