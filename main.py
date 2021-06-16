# %% imports
import time
# import logging

import sqlite3
import pandas as pd
from pypika import Query

import ray
from ray.util import ActorPool
from ray.util.queue import Queue
from ray.test_utils import SignalActor

from scipy.sparse import lil_matrix

from src.util import pickle_load, create_api
from src.remote import Scheduler, Fetcher, DataKeeper


# %% Reducer


@ray.remote
class Reducer:
    def __init__(self, chain_matrix):
        self.chain_matrix = chain_matrix
        self._conn = sqlite3.connect('./data/db.sqlite')

    def process_incremental(self, author_id):
        # process `authors` at index `author_id`
        pass


# %% Initialize Ray
ray.init()

# %% Initialize
DB_PATH = './data/db.sqlite'

credentials = pickle_load('./data/vault')
api_list = [create_api(cred) for cred in credentials]

conn = sqlite3.connect(DB_PATH)

# load matrix if exists otherwise create one
try:
    chain_matrix = pickle_load('./data/matrix')
except FileNotFoundError:
    job_size = conn.execute('select count(id) from jobs').fetchone()[0]
    # using sparse boolean matrix to save memory spaces
    chain_matrix = lil_matrix((job_size, job_size), dtype=bool)

# store chain matrix to ray shared memory pool and get the id
# chain_mat_id = ray.put(chain_matrix)
# jobs_ser_ref = ray.put(jobs_ser)

# %% Actors initialization
page_queue = Queue()
halt_signal = SignalActor.remote()

scheduler = Scheduler.remote(DB_PATH)
data_keeper = DataKeeper.remote(page_queue)
fetchers = [Fetcher.remote(api, page_queue, scheduler)
            for api in api_list]
fetcher_pool = ActorPool(fetchers)
# reducer = Reducer.remote(page_queue, chain_mat_id)

# %%
fetchers_job = [fetc.work.remote() for fetc in fetchers]
data_keeper_job = data_keeper.work.remote()

# %%
while True:
    try:
        time.sleep(600)
    except KeyboardInterrupt:
        # TODO send halt signal
        # wait for all activity
        ray.get([])

        break

# %% Cleaning Up
ray.shutdown()
# ray.init()
# %%
