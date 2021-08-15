import numpy as np
from icecream import ic
import time


def _write_row_raw(index, source, target, rows, cols):
    for i, j in zip(rows, cols):
        current_value = target[index, i]
        previous_value = target[index, j]
        result_row = source[i, j] + previous_value
        if current_value > 0:
            result_row = np.minimum(current_value, result_row)
        target[index, i] = result_row


def timed(func):
    def timed_function(*args):
        start = time.time()
        result = func(*args)
        stop = time.time()
        print(f'ETA | {func.__name__}: {stop-start}')
        return result
    return timed_function


@timed
def _get_influences(index, source, target, rows, cols):
    previous_values = target[index, cols].toarray()[0]
    results = np.ravel(source[rows, cols]) + previous_values
    del previous_values
    current_values = target[index, rows].toarray()[0]
    initialized = current_values > 0
    results[initialized] = np.minimum(
        current_values[initialized], results[initialized])
    return results


def batch_zip(chunk_size, size, iter_1, *iter_rest):
    iters = [iter_1, *iter_rest]
    for i in range(0, size, chunk_size):
        yield (it[i:i+chunk_size] for it in iters)


@timed
def get_followers(csc, previous_followers):
    return csc[:, previous_followers].nonzero()


def continuous_batch(chunk_size):
    pass


def process_row(index, target_matrix, pivot_matrices, batch_size=100000):
    csr, csc = pivot_matrices

    previous_followers = np.array([index])

    while True:
        iter_previous_followers = batch_zip(
            1000, previous_followers.shape[0], previous_followers)
        ic(iter_previous_followers)
        for prev in iter_previous_followers:
            followers, cols = get_followers(csc, prev)
            num_of_followers = followers.shape[0]
            if not num_of_followers:
                break
            ic(num_of_followers)
            parent_followers = previous_followers[cols]
            del cols

            iter_batch = batch_zip(
                batch_size, num_of_followers, followers, parent_followers)
            for rows, cols in iter_batch:
                influences = _get_influences(index, csr, target_matrix, rows, cols)
                target_matrix[index, rows] = influences

        # TODO: this dont work!
        previous_followers = followers
