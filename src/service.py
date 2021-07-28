from sqlalchemy.orm.session import Session
from src.model import Credential, Job, User
from cryptography.fernet import Fernet


class Service:
    def __init__(self, session: Session):
        self.session = session


class CredentialService(Service):
    def __init__(self, session: Session, fernet_key: str = None):
        super().__init__(session=session)

    def add_credential(self, api_key, secret, token):
        credential = Credential(api_key=api_key, secret=secret, token=token)
        self.session.add(credential)
        self.session.commit()

    def get_credentials(self):
        credentials = self.session.query(Credential).all()
        return credentials


class JobService(Service):
    def get_idle_jobs(self):
        return self.session.query(Job).filter(Job.status == 0).all()

    def get_all(self):
        return self.session.query(Job).all()


class UserService(Service):
    def get_unfetched(self):
        return self.session.query(User).filter(User.is_fetched == False).all()
