import hashlib
import pickle
import tweepy as tw
import logging

from enum import Enum


class StatusEnum(Enum):
    DONE = 1
    OTHER = -1
    NO_AUTH = 2
    ERR32 = 3


def get_hash(str_value, length=20):
    sha = hashlib.sha256(str_value.encode())
    hash_digest = sha.hexdigest()
    return hash_digest[:length]


def pickle_dump(data, filename):
    with open(f'{filename}.pkl', 'wb') as f:
        pickle.dump(data, f)


def pickle_load(filename):
    with open(f'{filename}.pkl', 'rb') as f:
        data = pickle.load(f)
    return data


def create_api(api_token, wait_on_rate_limit=False, wait_on_rate_limit_notify=False):
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


def create_logger():
    logging_format = f'%(asctime)s - %(levelname)s: %(message)s'
    logging_datefmt = '%d-%m-%y %H:%M:%S'

    logging.basicConfig(level=logging.INFO,
                        format=logging_format,
                        datefmt=logging_datefmt)
    fileformat = logging.Formatter(logging_format, datefmt=logging_datefmt)

    logger = logging.getLogger('myLogger')

    handler = logging.FileHandler('./logs/info.log')
    handler.setLevel(logging.INFO)
    handler.setFormatter(fileformat)
    logger.addHandler(handler)

    errorLogHandler = logging.FileHandler('./logs/error.log')
    errorLogHandler.setLevel(logging.ERROR)
    errorLogHandler.setFormatter(fileformat)
    logger.addHandler(errorLogHandler)

    return logger
