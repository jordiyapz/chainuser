from configparser import ConfigParser

from sqlalchemy.orm.session import Session
from src.model import Base, Job, User
from sqlalchemy.engine.base import Engine
from src.util import create_db_engine
from sqlalchemy.exc import IntegrityError
import pandas as pd


config = ConfigParser()
config.read('config.ini')


engine: Engine = create_db_engine(user=config['DB']['user'], password=config['DB']
                                  ['password'], host=config['DB']['host'], database=config['DB']['database'])

Base.metadata.create_all(engine)


with Session(engine) as session:
    df = pd.read_csv('data/vaksinasicovid3.csv', usecols=['user_id', 'author'])
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)

    # users = session.query(User).all()
    # screen_names = [user.screen_name for user in users]
    # res = df['author'].isin(screen_names)
    # print(df.loc[0])
    # user = session.query(User).filter(
    #     User.screen_name == df.loc[0, 'author']).one()
    # print(user.screen_name, user.id)
    sub_statement = session.query(User.screen_name)
    statement = session.query(Job.screen_name).filter(
        ~Job.screen_name.in_(sub_statement))
    screen_names = [u[0] for u in statement.all()]

users = df[df['author'].isin(screen_names)].itertuples(index=False, name=None)

ct = 0
with Session(engine) as session:
    for user in users:
        try:
            session.add(User(id=user[1], screen_name=user[0]))
            session.commit()
        except IntegrityError:
            session.rollback()
            ct += 1
            print(ct, *user)
        except KeyboardInterrupt:
            break
