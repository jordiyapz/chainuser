import mariadb
import threading
import tweepy as tw
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from datetime import datetime

import sqlalchemy
from sqlalchemy.exc import InterfaceError
from sqlalchemy.orm import scoped_session, Session

from src.util import create_api_v2
from src.model import Credential, Cursor, Error, FriendsId, User
from src.service import CredentialService
from src import session_factory

Session = scoped_session(session_factory)

print('Initializing...')
stop_event = threading.Event()
done_event = threading.Event()


def fetch_thread_routine(credential: Credential):
    thread_ident = credential.id     # threading.get_ident()

    try:
        with Session.begin():
            api: tw.API = create_api_v2(credential)

        while not stop_event.is_set():
            try:
                with Session.begin():
                    user = next(user_gen)
                print(f'T-{thread_ident} working on user: {user.screen_name}')
            except StopIteration:
                print(f'T-{thread_ident} All user has been fetched')
                done_event.set()
                break

            try:
                with Session.begin():
                    cursor = Session.query(Cursor).filter(
                        Cursor.user_id == user.id).one()
            except sqlalchemy.orm.exc.NoResultFound as ex:
                with Session.begin():
                    cursor = Cursor(user_id=user.id)
                    Session.add(cursor)

            with Session.begin():
                tw_cursor = tw.Cursor(api.friends_ids, cursor=cursor.next_cursor,
                                      user_id=user.id, screen_name=user.screen_name)

            pages = tw_cursor.pages()
            count = 0
            sleep_started = False

            try:
                while not stop_event.is_set():
                    try:
                        page = next(pages)
                        with Session.begin():
                            Session.bulk_save_objects(
                                [FriendsId(user_id=user.id, friend_id=friend_id) for friend_id in page])
                            cursor.next_cursor = str(pages.next_cursor)
                            Session.add(cursor)

                        count += len(page)
                        sleep_started = False

                    except StopIteration:
                        with Session.begin():
                            user.is_fetched = True
                            Session.add(user)
                            Session.delete(cursor)
                            print(
                                f'T-{thread_ident} User {user.screen_name} has {count} friends')
                        break

                    except tw.RateLimitError:
                        if not sleep_started:
                            print(f'T-{thread_ident} is sleeping [{datetime.now().strftime("%H:%M:%S")}]')
                            sleep_started = True
                        sleep(10)

                    except tw.TweepError as te:
                        print(
                            f'T-{thread_ident} ERROR: User {user.screen_name} got {str(te)}')
                        with Session.begin():
                            error = Error(full_message=str(te),
                                          user_id=user.id)

                            if str(te) == 'Not authorized.':
                                error.message = str(te)
                                error.code = -1
                            else:
                                try:
                                    t_err = te.args[0][0]
                                    error.code = t_err['code']
                                    error.message = t_err['message']
                                except Exception as e:
                                    print(e)
                            Session.add(error)
                        break

            except InterfaceError as ie:
                print(f'T-{thread_ident} Error: InterfaceError')
            except mariadb.InterfaceError as ie:
                print(f'T-{thread_ident} Error: mariadb.InterfaceError')
            except Exception as ex:
                print(f'T-{thread_ident} exception: {ex}')

        print(f'T-{thread_ident} quits.')

    except Exception as ex:
        print(f'T-{thread_ident} Killed with exception: {ex}')

    # Session.remove()


print('Loading database...')
session = Session()
# with Session() as session:
credentials = CredentialService(session).get_credentials()
# with Session() as session:
error_user_stmt = session.query(Error.user_id)
users = session.query(User).filter(~User.is_fetched).filter(
    ~User.id.in_(error_user_stmt)).all()
Session.remove()

# credentials = [credentials[-1]]
user_gen = (u for u in users)

print('Starting...')
with ThreadPoolExecutor(max_workers=len(credentials)) as executor:
    executor.map(fetch_thread_routine, credentials[:1])
    while not stop_event.is_set():
        try:
            if done_event.is_set():
                sleep(30)
                stop_event.set()
            else:
                sleep(10)
        except KeyboardInterrupt:
            stop_event.set()
            break
    if stop_event.is_set():
        print('Stopping...')

    # will call join

Session.remove()
print('Done.')
