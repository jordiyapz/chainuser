import pandas as pd
import sqlite3

chunksize = 50000


conn = sqlite3.connect('data/db.sqlite')

cols = list(pd.read_csv('data/friends.csv',
            chunksize=10).get_chunk().columns)[1:]

tp = pd.read_csv('data/friends.csv', chunksize=chunksize,
                 usecols=cols, iterator=True)

print('Starting...')
ct = 0
for df in tp:
    df.to_sql('friends', conn, index=None, if_exists='append')
    ct += len(df.index)
    print('Written', ct, 'rows')

conn.commit()
print('Finished!')
