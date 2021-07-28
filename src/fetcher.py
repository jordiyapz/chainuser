from time import sleep
from typing import Generator
from src.model import Credential
import tweepy as tw


class Fetcher:
    def __init__(self, credential: Credential, job_generator: Generator):
        auth = tw.OAuthHandler(credential.api_key, credential.secret)
        auth.set_access_token(credential.access_token,
                              credential.access_secret)
        self.api = tw.API(auth)
        self.job_gen = job_generator

    # def make_cursor(self, next_cursor=-1):
    #     # TODO: ambil kursor jika terdapat kursornya di tabel kursor
    #     # cursor = tw.Cursor(self.api.friends_ids, screen_name=self.author, count=200,
    #     #                    skip_status=True, include_user_entities=False, cursor=next_cursor)
    #     # self.cursor = cursor.pages()
        # pass

    def work(self):
        # print(self.api.auth.access_token[:5], next(self.job_gen))
        # sleep(1)
        pass
