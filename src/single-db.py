import pickle
import pandas as pd
import tweepy as tw
import logging
import time
import os.path
import re
import sqlite3

from src.util import *

logging_format = f'%(asctime)s - %(levelname)s: %(message)s'
logging_datefmt = '%d-%m-%y %H:%M:%S'
logging.basicConfig(level=logging.INFO,
                    format=logging_format,
                    datefmt=logging_datefmt)

fileformat = logging.Formatter(logging_format, datefmt=logging_datefmt)

logger = logging.getLogger('myLogger')
handler = logging.FileHandler('logs/mylog.log')
handler.setFormatter(fileformat)
logger.addHandler(handler)

errorLogHandler = logging.FileHandler('logs/error.log')
errorLogHandler.setLevel(logging.ERROR)
errorLogHandler.setFormatter(fileformat)
logger.addHandler(errorLogHandler)

conn = sqlite3.connect('data/db.sqlite')

creds = pickle_load('data/vault')
# authors = pickle_load('data/authors')
jobs = conn.execute('select screen_name from jobs where status=0').fetchall()
jobs = (name[0] for name in jobs)

MAX_WORKER = len(creds)


def set_status(screen_name, status=1):
    conn.execute(
        f'update jobs set status={status} where screen_name="{screen_name}"')
    conn.commit()


class Worker:
    def __init__(self, api):
        self.api = api
        self.cursor = None

        self.friends = []
        self.author = None
        self.last_checkpoint = None

    def create_cursor(self, next_cursor=-1):
        cursor = tw.Cursor(self.api.friends, screen_name=self.author, count=200,
                           skip_status=True, include_user_entities=False, cursor=next_cursor)
        self.cursor = cursor.pages()

    def fetch_friends(self):
        try:
            friend_page = self.cursor.next()
            self.friends.extend(friend_page)
            logger.info(f'[{self.author}] Fetched {len(self.friends)} friends')

        except tw.RateLimitError:
            if self.last_checkpoint is None or \
                    (time.time() - self.last_checkpoint) > 8 * 60:
                logger.info(
                    f'[{self.author}] Saving checkpoint {get_hash(self.author)}...')
                self.checkpoint()

        except StopIteration:
            logger.info(f'[{self.author}] Has {len(self.friends)} friends')
            self.finish()

        except tw.TweepError as te:
            logger.error(f'[{self.author}] {te}')
            err_msg = str(te)

            status = -1
            if err_msg == 'Not authorized.':
                status = 2
            elif err_msg[:12] == "[{'code': 34":
                status = 3
            elif re.match('Failed to send request: HTTPSConnectionPool', err_msg):
                status = 4

            set_status(self.author, status)
            self.next_()

    def checkpoint(self):
        hash_digest = get_hash(self.author)
        checkpoint_data = {'author': self.author, 'friends': self.friends,
                           'next_cursor': self.cursor.next_cursor}
        pickle_dump(checkpoint_data, f'checkpoints/{hash_digest}')
        self.last_checkpoint = time.time()

    def next_(self):
        self.author = next(jobs)
        logger.info(f'Working on author {self.author}')
        # check in checkpoints
        hash_digest = get_hash(self.author)
        checkpoint_name = f'checkpoints/{hash_digest}'
        if os.path.exists(checkpoint_name):
            logger.info(f'Loading checkpoint {hash_digest}...')
            with open(checkpoint_name, 'rb') as f:
                data = pickle.load(f)
            assert self.author == data['author']
            self.friends = data['friends']
            self.create_cursor(data['next_cursor'])
            self.last_checkpoint = time.time()
        else:
            self.friends = []
            self.last_checkpoint = None
            self.create_cursor()

    def finish(self):
        nf_df = pd.DataFrame((f._json for f in self.friends))
        nf_df['origin_friend'] = self.author
        nf_df.drop('id', axis=1, inplace=True)
        nf_df['withheld_in_countries'] = nf_df['withheld_in_countries'].apply(
            str)

        nf_df.to_sql('friends', conn, index=None, if_exists='append')

        set_status(self.author, 1)   # inside this is committing.

        try:
            hash_digest = get_hash(self.author)
            os.remove(f'checkpoints/{hash_digest}')
        except FileNotFoundError:
            pass

        self.next_()


# initialize workers
workers = [Worker(create_api(creds[i])) for i in range(MAX_WORKER)]
for worker in workers:
    worker.next_()

# infinite loops
while True:
    try:
        for worker in workers:
            worker.fetch_friends()
    except StopIteration:
        # All task finished (hopefully...)
        break
    except KeyboardInterrupt:
        logger.info('Saving to checkpoints...')
        for worker in workers:
            worker.checkpoint()
        break
