import ray
import sqlite3
import time
from scipy.sparse import lil_matrix

from src.util import pickle_load, pickle_dump


@ray.remote
class Reducer:
    def __init__(self, halt_signal):
        self._conn = sqlite3.connect('./data/db.sqlite')
        self._halt_signal = halt_signal

    def work(self):
        # load matrix if exists otherwise create one
        try:
            chain_matrix = pickle_load('./data/matrix')
        except FileNotFoundError:
            job_size = self._conn.execute(
                'select count(id) from jobs').fetchone()[0]
            # using sparse boolean matrix to save memory spaces
            chain_matrix = lil_matrix((job_size, job_size), dtype=bool)

        while True:
            try:
                # ambil id-id yang matriksnya ada isi
                nonzero, _ = chain_matrix.nonzero()
                nonzero = list(set(nonzero))

                # ambil nama dan id dari jobs yang tidak ada di nonzero dan statusnya 1
                authors = self._conn.execute(f'''
                    select id, screen_name
                    from jobs
                    where status=1
                        and id not in (
                            {','.join(map(str, nonzero))}
                        )
                        and screen_name in (
                            select distinct origin_friend
                            from friends
                        )
                ''').fetchall()

                if authors:
                    for id_, author in authors:
                        # ambil user-user yang difollow olehnya
                        ids = self._conn.execute(f'''
                        select id from jobs
                        where screen_name in (
                            select screen_name from friends
                            where origin_friend="{author}"
                        )
                        ''').fetchall()
                        ids = (i[0] for i in ids)
                        for i in ids:
                            chain_matrix[id_, i] = True
                    pickle_dump(chain_matrix, './data/matrix')
                    print(f'{[au for _, au in authors]} chains saved')
                elif ray.get(self._halt_signal.get.remote()):
                    break
                else:
                    ct = 0
                    while not ray.get(self._halt_signal.get.remote()):
                        time.sleep(10)
                        ct += 1
                        if ct >= 3:
                            break
            except Exception as e:
                print(e)
                break
