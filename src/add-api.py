from src import session_factory
from src.model import Credential

api_key = input('API key: ')
secret = input('API secret: ')
access_token = input('Access token: ')
access_secret = input('Access token secret: ')

new_credential = Credential(api_key=api_key, secret=secret,
                            access_token=access_token, access_secret=access_secret)

with session_factory() as session, session.begin():
    session.add(new_credential)

print('Credential added')
