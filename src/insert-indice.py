from src.model import User
from sqlalchemy.orm.scoping import scoped_session
from src import session_factory
from sqlalchemy.sql.expression import func
from tqdm import tqdm


def Pagination(gen, num):
    stop = False
    while not stop:
        items = []
        for _ in range(num):
            try:
                item = next(gen)
                items.append(item)
            except StopIteration:
                stop = True
                break
        yield (item for item in items)


Session = scoped_session(session_factory)

with Session.begin():
    print('Loading users...')
    users = Session.query(User).filter(User.indice == None).order_by(User.screen_name).all()
    indice = Session.query(func.max(User.indice)).one()[0] + 1

num_items = 1000
page_gen = Pagination((u for u in users), num_items)
starting_indice = indice

print('Working...')
user = None
while True:
    try:
        with Session.begin():
            for user in next(page_gen):
                user.indice = indice
                indice += 1
            if (user):
                print(user.screen_name, user.indice)
    except StopIteration:
        print('Done')
        with Session.begin():
            count = Session.query(func.count(User.id)).filter(User.indice != None).one()
        print(f'Processed {count} users')
        break
    except KeyboardInterrupt:
        break

print(f'Processed {indice - num_items - starting_indice} items')

# should_stop = False

# print('Starting...')
# while not should_stop:
#     with Session.begin():
#         while True:
#             try:
#                 i, user = next(gen);
#                 if user.indice == None:
#                     user.indice = i
#                 if i % 2000 == 0:
#                     print(i)
#                     break
#             except KeyboardInterrupt:
#                 should_stop = True
#                 break


Session.remove()