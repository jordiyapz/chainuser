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


keys = ['origin_friend', 'id_str', 'name', 'screen_name', 'location', 'url',
        'description', 'protected', 'followers_count', 'friends_count', 'listed_count',
        'created_at', 'favourites_count', 'verified', 'statuses_count',
        'contributors_enabled', 'profile_image_url_https', 'profile_banner_url',
        'default_profile', 'default_profile_image', 'live_following', 'muting',
        'blocking', 'blocked_by', 'withheld_in_countries']


def save_data(conn, data, commit=True):
    cur = conn.cursor()
    sql = f'''
        insert into friends ({",".join(keys)})
        values ({",".join(["?"]*len(keys))})
        '''
    cur.executemany(sql, data)
    if commit:
        conn.commit()


def parse_data(friends, author):
    parsed_data = []

    for friend in friends:
        f_json = friend._json
        f_json['origin_friend'] = author
        f_json['withheld_in_countries'] = str(f_json['withheld_in_countries'])
        parsed = tuple(f_json.get(key, None) for key in keys)

        parsed_data.append(parsed)

    return parsed_data


def set_status(conn, screen_name, status=1):
    cur = conn.cursor()
    cur.execute('update jobs set status=? where screen_name=?',
                (status, screen_name))
    conn.commit()
