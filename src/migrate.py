import pandas as pd
import sqlite3
import mariadb
from tqdm import trange

BATCH_SIZE = 10000

maria_conn = mariadb.connect(user='jordi', password='jedryn', 
							 database='chainuser')
maria_conn.autocommit = True
maria_cur = maria_conn.cursor()

sqlite_conn = sqlite3.connect('data/db.sqlite')

maria_cur.execute('''
	CREATE TABLE IF NOT EXISTS friends(
	    id integer auto_increment primary key,
	    origin_friend varchar(30),
	    id_str varchar(20),
	    name varchar(100),
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
maria_cur.execute('''
	CREATE TABLE IF NOT EXISTS jobs (
		id integer primary key,
		screen_name varchar(40),
		status int default 0
	)
	''')

jobs = sqlite_conn.execute('select * from jobs').fetchall()

# replaces
maria_cur.execute('delete from jobs')
maria_cur.executemany('insert into jobs values (?, ?, ?)', jobs)

sqlite_conn.execute('PRAGMA synchronize = 0')

total_rows = sqlite_conn.execute('SELECT max(id) FROM friends').fetchone()[0]


for i in trange(0, total_rows, BATCH_SIZE):
	cur = sqlite_conn.execute('''
		SELECT id, origin_friend, id_str, name, screen_name, location, 
			url, description, protected, followers_count, friends_count, 
			listed_count, created_at, favourites_count, verified, 
			statuses_count, contributors_enabled, profile_image_url_https, 
			profile_banner_url, default_profile, default_profile_image, 
			live_following, muting, blocking, blocked_by, 
			withheld_in_countries
		FROM friends 
		LIMIT ? 
		OFFSET ?
		''', (BATCH_SIZE, i))

	rows = cur.fetchall()
	maria_cur.executemany(f'''
		INSERT INTO friends (
			id, origin_friend, id_str, name, screen_name, location, 
			url, description, protected, followers_count, friends_count, 
			listed_count, created_at, favourites_count, verified, 
			statuses_count, contributors_enabled, profile_image_url_https, 
			profile_banner_url, default_profile, default_profile_image, 
			live_following, muting, blocking, blocked_by, 
			withheld_in_countries
		)
		VALUES ({','.join(['?']*26)})''', rows)

sqlite_conn.close()
maria_conn.close()
