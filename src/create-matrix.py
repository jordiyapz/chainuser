from src.model import FriendsId, User
from sqlalchemy.orm.scoping import scoped_session
from collections import OrderedDict
from src import session_factory

Session = scoped_session(session_factory=session_factory)

with Session.begin():
    users = Session.query(User.id, User.screen_name).order_by(User.screen_name).all()
    users_dt = dict(users)

print(users_dt[0])