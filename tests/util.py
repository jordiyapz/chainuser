from configparser import ConfigParser
import mariadb

config = ConfigParser()
config.read('config.ini')


def setup_db():
    conn = mariadb.connect(user=config['DB']['USER'], password=config['DB']['PASSWORD'],
                           host=config['DB']['HOST'], database=config['TEST']['DB_NAME'])

    return conn


def teardown_db(conn, cursor, tables):
    for t in tables:
        cursor.execute(f'drop table {t}')
    conn.close()


class TestSuite:
    def __init__(self):
        self.tests = []

    def add_test(self, func):
        self.tests.append(func)
        return func

    def run(self):
        for test in self.tests:
            test()
