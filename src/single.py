import pickle
import pandas as pd
import tweepy as tw
import logging
import time
import os.path
import re

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

creds = pickle_load('data/vault')
authors = pickle_load('data/authors')

MAX_WORKER = len(creds)

author_generator = (author for author in authors if authors[author] == 0)


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
            pickle_dump(authors, 'data/authors')
        except tw.TweepError as te:
            logger.error(f'[{self.author}] {te}')
            err_msg = str(te)
            if err_msg == 'Not authorized.':
                authors[self.author] = 2
            elif err_msg[:12] == "[{'code': 34":
                authors[self.author] = 3
            elif re.match('Failed to send request: HTTPSConnectionPool', err_msg):
                authors[self.author] = 4
            else:
                authors[self.author] = -1
            pickle_dump(authors, 'data/authors')
            self.next_()

    def checkpoint(self):
        hash_digest = get_hash(self.author)
        checkpoint_data = {'author': self.author, 'friends': self.friends,
                           'next_cursor': self.cursor.next_cursor}
        pickle_dump(checkpoint_data, f'checkpoints/{hash_digest}')
        self.last_checkpoint = time.time()

    def next_(self):
        self.author = next(author_generator)
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
        new_friend_df = pd.DataFrame((f._json for f in self.friends))
        new_friend_df['origin_friend'] = self.author
        # friend_df = pd.read_csv('friends.csv')
        # friend_df.append(new_friend_df).to_csv('friends.csv', index=None)
        new_friend_df.to_csv('data/friends.csv', mode='a',
                             index=None, header=None)
        authors[self.author] = 1
        hash_digest = get_hash(self.author)
        try:
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
        pickle_dump(authors, 'data/authors')
        break
    except KeyboardInterrupt:
        logger.info('Saving to checkpoints...')
        for worker in workers:
            worker.checkpoint()
        pickle_dump(authors, 'data/authors')
        break
