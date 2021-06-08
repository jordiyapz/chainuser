import hashlib
import pickle


def get_hash(str_value, length=20):
    sha = hashlib.sha256(str_value.encode())
    hash_digest = sha.hexdigest()
    return hash_digest[:length]


def pickle_dump(data, filename):
    with open(f'{filename}.pkl', 'wb') as f:
        pickle.dump(data, f)


def pickle_load(filename):
    with open(f'{filename}.pkl', 'rb') as f:
        data = pickle.load(f)
    return data
