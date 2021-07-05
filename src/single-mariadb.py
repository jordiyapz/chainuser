import pickle
import pandas as pd
import tweepy as tw
import logging
import time
import os.path
import re
import mariadb
from configparser import ConfigParser

from src.util import *


def fetch_jobs(conn):
    cur = conn.cursor()
    cur.execute('select screen_name from jobs where status=0')
    return cur.fetchall()


config = ConfigParser()
if not config.read('config.ini'):
    raise FileNotFoundError('File "config.ini" not found')


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

conn = mariadb.connect(user=config['DB']['USER'], password=config['DB']['PASSWORD'],
                       host=config['DB']['HOST'], database=config['DB']['NAME'])

creds = pickle_load('data/vault')


jobs = (name[0] for name in fetch_jobs(conn))

MAX_WORKER = len(creds)


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

            set_status(conn, self.author, status)
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
        checkpoint_name = f'checkpoints/{hash_digest}.pkl'
        # TODO: Fix Checkpoint issue
        if os.path.exists(checkpoint_name):
            with open(checkpoint_name, 'rb') as f:
                data = pickle.load(f)
            assert self.author == data['author']
            self.friends = data['friends']
            self.create_cursor(data['next_cursor'])
            self.last_checkpoint = time.time()
            logger.info(
                f'Loaded {len(self.friends)} {self.author}\'s friends from {hash_digest}')
        else:
            self.friends = []
            self.last_checkpoint = None
            self.create_cursor()

    def finish(self):
        try:
            if len(self.friends) > 0:
                save_data(conn, parse_data(
                    self.friends, self.author), commit=False)
            set_status(conn, self.author, 1)   # inside this is committing.
            try:
                hash_digest = get_hash(self.author)
                os.remove(f'checkpoints/{hash_digest}')
            except FileNotFoundError:
                pass
        except mariadb.InterfaceError as error:
            if error.message == 'Lost connection to MySQL server during query':
                print(error.message)
                self.checkpoint()
            print(error)

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

conn.close()
