# %% imports
from src.remote import Scheduler, Fetcher, DataKeeper, Reducer, HaltSignalActor
from src.util import pickle_load, create_api
from ray.util.queue import Queue
import ray
import sqlite3
import time

# %% Initialize Ray
ray.init()

# %% Initialize
credentials = pickle_load('./data/vault')
api_list = [create_api(cred) for cred in credentials]

DB_PATH = './data/db.sqlite'
conn = sqlite3.connect(DB_PATH)

# %% Actors initialization
page_queue = Queue()
halt_signal = HaltSignalActor.remote()

scheduler = Scheduler.remote(DB_PATH)
data_keeper = DataKeeper.remote(page_queue, halt_signal)
fetchers = [Fetcher.remote(api, page_queue, scheduler, halt_signal)
            for api in api_list]
reducer = Reducer.remote(halt_signal)

# %% WORK!!!
fetcher_jobs = [fetc.work.remote() for fetc in fetchers]
data_keeper_job = data_keeper.work.remote()
reducer_job = reducer.work.remote()

# %% Looping forever
while True:
    try:
        time.sleep(600)
    except KeyboardInterrupt:
        try:
            # send halt signal
            halt_signal.send.remote()
            print('\nPlease wait up to 10 seconds... ')
            # wait for all activity
            ray.get([*fetcher_jobs,
                    data_keeper_job, reducer_job])
        except Exception as e:
            pass

        break

# %% Cleaning Up
print('Shutting down...')
try:
    time.sleep(1)
    ray.shutdown()
except Exception as e:
    pass
