from configparser import ConfigParser
from src.model import Base
from sqlalchemy.engine.base import Engine

from sqlalchemy.orm.session import Session
from src.fetcher import Fetcher
from src.service import CredentialService, UserService
from src.util import create_db_engine

config = ConfigParser()
config.read('config.ini')


engine: Engine = create_db_engine(user=config['DB']['user'], password=config['DB']
                                  ['password'], host=config['DB']['host'], database=config['DB']['database'])

Base.metadata.create_all(engine)

with Session(engine) as session:
    print('Loading data...')
    credentials = CredentialService(session).get_credentials()
    unfetched_users = UserService(session).get_unfetched()
    job_generator = (user.screen_name for user in unfetched_users)

fetchers = [Fetcher(credential, job_generator) for credential in credentials]

# while True:
#     try:
#         for fetcher in fetchers:
#             fetcher.work()
#     except KeyboardInterrupt:
#         break
