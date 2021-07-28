from configparser import ConfigParser

from sqlalchemy.orm.session import sessionmaker

from src.model import Base, Cursor

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine


config = ConfigParser()
config.read('config.ini')
db_conf = config['DB']

engine: Engine = create_engine(
    f'mariadb+mariadbconnector://{db_conf["user"]}:{db_conf["password"]}@{db_conf["host"]}:3306/{db_conf["database"]}',
    pool_pre_ping=True, echo=False)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

with Session() as session:
    cursors = session.query(Cursor).all()
    print([(cur.user_id, cur.next_cursor) for cur in cursors])

    cursor = cursors[1]

with Session.begin() as session:
    print(cursor.next_cursor)
    cursor.next_cursor = "-1"
    session.add(cursor)
    cursor.next_cursor = '-2'
    session.add(cursor)

with Session.begin() as session:
    cursor.next_cursor = '-3'
    session.add(cursor)
    print(cursor.next_cursor)

