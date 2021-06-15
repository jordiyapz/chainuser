import ray
import sqlite3 as sql
import pandas as pd
import time


@ray.remote
class DataKeeper:
    def __init__(self, page_queue) -> None:
        self._page_queue = page_queue
        self._conn = sql.connect('data/db.sqlite')

    def work(self) -> None:
        while True:
            if not self._page_queue.empty():
                rows = []
                while not self._page_queue.empty():
                    # proses tiap data supaya sesuai dengan database
                    author, page = self._page_queue.get()
                    for user in page:
                        user = user._json
                        user['origin_friend'] = author
                        rows.append(user)

                df = pd.DataFrame(rows)
                df.drop('id', axis=1, inplace=True)
                df['withheld_in_countries'] = df['withheld_in_countries'].apply(
                    str)

                df.to_sql('friends', con=self._conn,
                          if_exists='append', index=None)
                self._conn.commit()

                # next, do check whether queue is empty

            else:
                # add some throttle to reduce cpu usage
                # Make sure to abort this in emergency mode
                time.sleep(300)  # sleep 5 sec
