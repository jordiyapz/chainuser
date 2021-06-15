# %% imports
import time
# import logging
import tweepy as tw

import sqlite3 as sql
import pandas as pd

import ray
from ray.util import ActorPool
from ray.util.queue import Queue
from ray.test_utils import SignalActor

from scipy.sparse import lil_matrix

from src.util import pickle_load, create_api, StatusEnum
from src.remote import Scheduler, Fetcher, DataKeeper


# %% Reducer


@ray.remote
class Reducer:
    def __init__(self, chain_matrix):
        self.chain_matrix = chain_matrix
        self._conn = sql.connect('./data/db.sqlite')

    def process_incremental(self, author_id):
        # process `authors` at index `author_id`
        pass


# %% Initialize Ray
ray.init()

# %% Initialize
credentials = pickle_load('./data/vault')
author_map = pickle_load('./data/authors')

api_list = [create_api(cred) for cred in credentials]
author_ser = pd.Series(author for author in author_map)
num_of_authors = len(author_ser.index)

# load matrix if exists otherwise create one
try:
    chain_matrix = pickle_load('./data/matrix')
except FileNotFoundError:
    # using sparse boolean matrix to save memory spaces
    chain_matrix = lil_matrix((num_of_authors, num_of_authors), dtype=bool)

# store chain matrix to ray shared memory pool and get the id
chain_mat_id = ray.put(chain_matrix)


# %% Actors initialization
scheduler = Scheduler.remote(author_map)
page_queue = Queue()
halt_signal = SignalActor.remote()
dk = DataKeeper.remote(page_queue)
fetchers = [Fetcher.remote(api, page_queue, scheduler)
            for api in api_list]
fetcher_pool = ActorPool(fetchers)
# reducer = Reducer.remote(page_queue, chain_mat_id)

# %%
fetchers_job = [fetc.work.remote() for fetc in fetchers]
dk_job = dk.work.remote()

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
