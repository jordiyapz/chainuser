from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Boolean, String, Integer, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Credential(Base):
    __tablename__ = 'credentials'
    id = Column(Integer, primary_key=True, autoincrement=True)
    api_key = Column(String(30), nullable=False)
    secret = Column(String(50), nullable=False)
    access_token = Column(String(255), nullable=False)
    access_secret = Column(String(255), nullable=False)

class Job(Base):
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key=True)
    screen_name = Column(String(40))
    status = Column(Integer, default=0)

class User(Base):
    __tablename__ = 'users'
    id = Column(String(20), primary_key=True)
    screen_name = Column(String(40))
    is_fetched = Column(Boolean, default=False)
    matrix_progress = Column(Integer, default=0)
    indice = Column(Integer)

    # friends = relationship('FriendsId')


class FriendsId(Base):
    __tablename__ = 'friends_ids'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(20), ForeignKey(User.id))
    friend_id = Column(String(20), nullable=False)

    # user = relationship('User')

class Error(Base):
    __tablename__ = 'errors'
    id = Column(Integer, primary_key=True, autoincrement=True)
    message = Column(Text)
    full_message = Column(Text)
    user_id = Column(String(20), ForeignKey(User.id))
    code = Column(Integer)

    user = relationship('User', foreign_keys='Error.user_id')

class Cursor(Base):
    __tablename__ = 'cursors'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(20), ForeignKey(User.id))
    next_cursor = Column(String(20), default='-1')

    user = relationship('User', foreign_keys='Cursor.user_id')
