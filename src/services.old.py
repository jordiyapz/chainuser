from pypika import Query, Table


class Jobs:
    def __init__(self, conn):
        self.conn = conn
        self.t = Table('jobs')

    def get_id(self, screen_name):
        q = Query.from_(self.t).select(self.t.id).where(
            self.t.screen_name == screen_name)
        ids = self.conn.execute(q.get_sql()).fetchone()
        if len(ids) == 0:
            return None
        return ids[0]

    def set_job(self, job_id, status):
        q = Query.update(self.t).set(self.t.status, status).where(
            self.t.id == job_id)
        self.conn.execute(q.get_sql()).fetchone()

        self.conn.commit()

    def get_by_status(self, status=0):
        q = Query.from_(self.t).select(
            self.t.screen_name).where(self.t.status == status)
        res = self.conn.execute(q.get_sql()).fetchall()
        return (j[0] for j in res)
