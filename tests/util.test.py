from src.util import parse_data, keys, save_data, set_status
import pickle
from tests.util import *


test_suite = TestSuite()

with open('tests/checkpoint.pkl', 'rb') as f:
    data = pickle.load(f)

friends = data['friends']
author = data['author']


@test_suite.add_test
def test_parse_data():
    parsed_data = parse_data(friends, author)
    assert type(parsed_data) is list
    assert len(parsed_data) == len(friends)

    assert type(parsed_data[0]) is tuple

    for i, key in enumerate(keys):
        assert friends[0]._json.get(key, None) == parsed_data[0][i]

    print('test_parse_data PASSED')


@test_suite.add_test
def test_save_data():
    conn = setup_db()
    try:
        cur = conn.cursor()
        cur.execute('''
        CREATE TABLE IF NOT EXISTS friends(
            id integer auto_increment primary key,
            origin_friend varchar(30),
            id_str varchar(20),
            name varchar(100)
                CHARACTER SET utf8mb4
                COLLATE utf8mb4_unicode_ci,
            screen_name varchar(40),
            location text,
            url text,
            description text,
            protected boolean,
            followers_count int,
            friends_count int,
            listed_count int,
            created_at text,
            favourites_count int,
            verified boolean,
            statuses_count int,
            contributors_enabled integer,
            profile_image_url_https text,
            profile_banner_url text,
            default_profile boolean,
            default_profile_image boolean,
            live_following integer,
            muting integer,
            blocking integer,
            blocked_by text,
            withheld_in_countries text
        )
        ''')
        parsed_data = parse_data(friends, author)
        print('Saving...')
        save_data(conn, parsed_data)

        cur = conn.cursor()
        cur.execute('select max(id) from friends')
        data_len = cur.fetchone()[0]
        assert data_len == len(parsed_data)

        cur.execute(
            'select origin_friend, id_str, name, screen_name from friends limit 10')
        data = cur.fetchall()
        for i, ori in enumerate(parsed_data[:10]):
            for j in range(4):
                assert ori[j] == data[i][j], f'Expect {ori[j]} and {data[i][j]} to be equals'

        print('test_save_data PASSED')

    except Exception as ex:
        print(ex)
        print('test_save_data FAILED')

    finally:
        teardown_db(conn, cur, ('friends',))


@test_suite.add_test
def test_set_status():
    try:
        conn = setup_db()
        cur = conn.cursor()
        cur.execute('''
        create table if not exists jobs (
            id int primary key auto_increment,
            screen_name varchar(30),
            status int default 0
        )
        ''')

        cur.executemany('insert into jobs (screen_name) values (?)',
                        [('alice',), ('bob',), ('charlie',)])
        target_name = 'alice'
        target_status = 1
        set_status(conn, target_name, status=target_status)

        cur.execute('select status from jobs where screen_name=?',
                    (target_name,))
        status = cur.fetchone()[0]

        assert status == target_status, f'{status} not equals {target_status}'

        print('test_set_status PASSED')
    except Exception as ex:
        print(ex)
        print('test_set_status FAILED')
    finally:
        teardown_db(conn, cur, ('jobs', ))


test_suite.run()

print('DONE')
