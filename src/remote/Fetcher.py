from src.util import StatusEnum
import ray
from ray.util.queue import Queue
import tweepy as tw
import time

from src.remote import Scheduler


@ray.remote
class Fetcher:
    def __init__(self, api, page_queue: Queue, scheduler: Scheduler):
        self._api = api
        self._cursor = None
        self._author = None

        self._page_queue = page_queue
        self._scheduler = scheduler
        # self._logger = logger

    def _create_cursor(self, next_cursor=-1):
        cursor = tw.Cursor(self._api.friends, screen_name=self._author, count=200,
                           skip_status=True,  include_user_entities=False, cursor=next_cursor)
        self._cursor = cursor.pages()

    def _get_next(self, next_cursor=-1):
        self._author = ray.get(self._scheduler.get_next.remote())
        self._create_cursor(next_cursor)
        print(f'Working on `{self._author}`')

    def work(self, next_cursor=-1):
        try:
            self._get_next(next_cursor)
            while True:
                should_get_next = False
                try:
                    page = self._cursor.next()
                    self._page_queue.put_nowait((self._author, page))
                    print(f'{self._author}: {len(page)}')

                except tw.RateLimitError:
                    print('RateLimit')
                    time.sleep(900)

                except StopIteration:
                    should_get_next = True
                    self._scheduler.set_job(self._author, StatusEnum.DONE)

                except tw.TweepError as te:
                    should_get_next = True
                    print('TweepError:', te)
                    self._scheduler.set_job(self._author, StatusEnum.OTHER)

                if should_get_next:
                    try:
                        self._get_next(next_cursor)
                    except Exception as e:
                        print(e)
        except Exception as e:
            print(e)
