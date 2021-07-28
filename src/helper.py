from configparser import ConfigParser
from sqlalchemy.engine import create_engine

from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import sessionmaker
from src.model import Base, User
from scipy.sparse import lil_matrix, load_npz, save_npz


def connect_db():
    config = ConfigParser()
    config.read('config.ini')
    db_conf = config['DB']

    print('Connecting...')
    engine: Engine = create_engine(
        f'mariadb+mariadbconnector://{db_conf["user"]}:{db_conf["password"]}@{db_conf["host"]}:{db_conf["port"]}/{db_conf["database"]}',
        pool_pre_ping=True, echo=False,)
    # wait_timeout=28800,)
    # interactive_timeout=28800,)
    # connect_timeout=1000,)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    return session_factory

def load_matrix(filename, shape=(38789, 38789), dtype=bool):
    try:
        matrix = load_npz(filename).tolil()
    except IOError:
        matrix = lil_matrix(shape, dtype=dtype)
    return matrix


def save_matrix(filename, matrix):
    save_npz(filename, matrix.tocoo(copy=False), compressed=True)


def iter_users(session):
    while True:
        with session.begin():
            user = session.query(User).filter(
                User.is_fetched, User.matrix_progress == 0).order_by(User.indice).limit(1).one_or_none()
            if user:
                user.matrix_progress = 1

        if user is None:
            break
        yield user
