import ray


@ray.remote
class Scheduler:
    def __init__(self, author_map):
        self._author_map = author_map
        self.schedule = (job for job in author_map if author_map[job] == 0)

    def get_next(self):
        return next(self.schedule)
