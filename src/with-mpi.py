import pandas as pd
import tweepy as tw
from mpi4py import MPI
import pickle
import logging
from enum import Enum


class TagEnum(Enum):
    FLAG = 0


def create_api(api_token, wait_on_rate_limit=True, wait_on_rate_limit_notify=True):
    # auth
    auth = tw.OAuthHandler(
        api_token['consumer_key'], api_token['consumer_secret'])
    auth.set_access_token(
        api_token['access_token'], api_token['access_token_secret'])

    # api
    api = tw.API(auth,
                 wait_on_rate_limit=wait_on_rate_limit,
                 wait_on_rate_limit_notify=wait_on_rate_limit_notify)
    return api


with open('vault.pkl', 'rb') as f:
    creds = pickle.load(f)

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
total_process = comm.Get_size()

logging.basicConfig(level=logging.INFO,
                    format=f'%(asctime)s - R{rank} - %(levelname)s: %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


# for example if there are 2 creds, only rank1 and rank2 can have them.
# logging.error('Credentials not enough!')
assert rank <= len(creds), logging.error('Credentials not enough!')

role = 'manager' if rank == 0 else 'fetcher'

with open('authors.pkl', 'rb') as f:
    authors = pickle.load(f)

if role == 'manager':
    # Rank 0 is the manager
    # He doesn't work. He only show what other worker need to do.
    author_gen = (author for author in authors if not authors[author])

    req = comm.irecv(source=1, tag=TagEnum.FLAG)

elif role == 'fetcher':
    api = create_api(creds[rank-1])
