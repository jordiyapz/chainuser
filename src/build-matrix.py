from concurrent.futures.thread import ThreadPoolExecutor
from threading import Event, Lock, Thread
from time import sleep

from src.helper import iter_users, load_matrix, save_matrix
from src.model import FriendsId, User
from sqlalchemy.orm.scoping import scoped_session
from src import session_factory
from queue import Queue

MAT_FILE = 'data/matrix.npz'
NUM_OF_THREADS = 10

Session = scoped_session(session_factory)
matrix = load_matrix(MAT_FILE)
stop_event = Event()
lock = Lock()
user_iterator = iter_users(session=Session)
processed = Queue()


def mapper(id):
    try:
        while not stop_event.is_set():
            try:
                lock.acquire()
                user = next(user_iterator)
                lock.release()
            except StopIteration:
                lock.release()
                break
            except Exception as ex:
                print(f'T-{id} Iterator exception: {ex}')
                lock.release()
                break

            try:
                with Session.begin():
                    indices = Session.query(User.indice).select_from(User).join(
                        FriendsId, User.id == FriendsId.friend_id).filter(FriendsId.user_id == user.id).all()
                    indices = [indice for (indice, ) in indices]
                    matrix[user.indice, indices] = True
                    processed.put(user.id)
                    print(
                        f'T-{id} {user.indice}. {user.screen_name}: {len(indices)}')
            except Exception as ex:
                print(f'T-{id} Exception occured in loops: {ex}')
                break
    except Exception as ex:
        print(f'T-{id} Exception occured: {ex}')

    print(f'T-{id} quits.')


def writer_thread():
    while not processed.empty() or not stop_event.is_set():
        sleep(30)

        print('Writting...')
        user_ids = []
        while not processed.empty():
            user_id = processed.get()
            user_ids.append(user_id)

        save_matrix(MAT_FILE, matrix)
        with Session.begin():
            Session.query(User).filter(User.id.in_(user_ids)
                                       ).update({'matrix_progress': 2})
    print('Writter quits.')


Session.remove()


print('Starting...')
with ThreadPoolExecutor(max_workers=NUM_OF_THREADS) as executor:
    executor.map(mapper, range(NUM_OF_THREADS))

    writer = Thread(target=writer_thread, name='writer')
    writer.start()

    while not stop_event.is_set():
        try:
            sleep(10)
        except KeyboardInterrupt:
            stop_event.set()

    writer.join()

try:
    print('Saving...')
    save_matrix(MAT_FILE, matrix)

    user_ids = []
    while not processed.empty():
        user_id = processed.get()
        user_ids.append(user_id)

    if len(user_ids):
        with Session.begin():
            Session.query(User).filter(User.id.in_(user_ids)
                                       ).update({'matrix_progress': 2})

    with Session.begin():
        Session.query(User).filter(User.matrix_progress ==
                                   1).update({'matrix_progress': 0})

except KeyboardInterrupt:
    print('Please wait')
except Exception as ex:
    print(ex)

print('Done.')

Session.remove()
