import numpy as np
from src.helper import connect_db, load_matrix, save_matrix
from src.model import User

import pandas as pd
from datetime import datetime


def get_epoch(date):
    utc = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    return int((utc - datetime(2021, 1, 1)).total_seconds())

user_df_filename = 'data/process.pkl'
tweet_df_filename = 'data/vaksinasi_simplified.pkl'
chain_matrix_filename= 'data/chainmatrix.npz'

print('Loading...')
try:
    user_df = pd.read_pickle(user_df_filename)
except FileNotFoundError:
    Session = connect_db()
    with Session() as session, session.begin():
        users = session.query(User.screen_name, User.indice).all()
    user_df = pd.DataFrame((i for _, i in users),
                           columns=['indice'], index=(name for name, _ in users))
    del users
    user_df['processed_at'] = np.nan
    user_df = user_df.astype({'indice': int})
    user_df.sort_values('indice', inplace=True)
    user_df.to_pickle(user_df_filename)

try:
    tweet_df = pd.read_pickle(tweet_df_filename)
except FileNotFoundError:
    tweet_df = pd.read_csv('data/vaksinasicovid3_simplified.csv',
                           usecols=['author', 'pubdate'])
    tweet_df['epoch'] = tweet_df['pubdate'].apply(get_epoch)
    tweet_df = tweet_df.join(user_df, on='author')
    tweet_df = tweet_df.loc[tweet_df['indice'].dropna(
        axis=0).index, ['indice', 'author', 'epoch']]
    tweet_df = tweet_df.astype({'indice': int})
    tweet_df.to_pickle(tweet_df_filename)

relationship_matrix = load_matrix('data/matrix.npz').tocsr()
chain_matrix = load_matrix(chain_matrix_filename,
                           shape=relationship_matrix.shape, dtype=np.float32)

print('Starting...')
last_index = user_df['processed_at'].max(skipna=True)
last_index = 0 if np.isnan(last_index) else last_index
tuples = tweet_df.loc[last_index:,
                      ['indice', 'author', 'epoch']].itertuples(name=None)
for index, indice, author, epoch in tuples:
    try:
        if not np.isnan(user_df.at[author, 'processed_at']):
            continue
        relationships = relationship_matrix[indice].nonzero()[1]
        df = tweet_df[(tweet_df['epoch'] < epoch) &
                        (tweet_df['indice'].isin(relationships))]
        if len(df.index) == 0:
            continue
        print(index, author, len(df.index))
        df = df.groupby('indice').mean()
        chain_matrix[indice, df.index] = epoch - df['epoch']
        user_df.at[author, 'processed_at'] = index
    except KeyboardInterrupt:
        break

print('Saving...')
save_matrix(chain_matrix_filename, chain_matrix)
user_df.to_pickle(user_df_filename)

print('Matrix elements count:', chain_matrix.count_nonzero())
