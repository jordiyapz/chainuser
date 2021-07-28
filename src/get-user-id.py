from configparser import ConfigParser
from src.service import CredentialService, JobService
from src.util import create_api_v2, create_db_engine
from sqlalchemy.engine.base import Engine
from src.model import Base, User
from sqlalchemy.orm.session import Session
import tweepy as tw
from tqdm import tqdm


config = ConfigParser()
config.read('config.ini')

engine: Engine = create_db_engine(user=config['DB']['user'], password=config['DB']
                                  ['password'], host=config['DB']['host'], database=config['DB']['database'])

Base.metadata.create_all(engine)


with Session(engine) as session:
    credentials = CredentialService(session).get_credentials()
    api = create_api_v2(credentials[0])

    jobs = JobService(session).get_all()
    screen_names = [job.screen_name for job in jobs]
    packs = (screen_names[i*100:min((i+1)*100, len(screen_names))]
             for i in range(0, len(screen_names)//100 + 1))

for names in tqdm(packs, total=len(screen_names)//100 + 1):
    try:
        with Session(engine) as session, session.begin():
            try:
                users = api.lookup_users(screen_names=names, include_entities=False,
                                         tweet_mode=True)
                for user in users:
                    session.add(
                        User(id=user.id_str, screen_name=user.screen_name))
            except tw.TweepError as te:
                print(te)
    except KeyboardInterrupt:
        break
