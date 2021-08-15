import unittest
import numpy as np
from scipy.sparse import coo_matrix, lil_matrix
from src.influence_matrix import util


class TestCase(unittest.TestCase):
    def setUp(self):
        row = np.array([1, 2, 2, 3, 3, 5])
        col = np.array([4, 0, 5, 0, 5, 0])
        data = np.array([280, 300, 160, 130, 40, 100])
        self.chain_matrix = coo_matrix(
            (data, (row, col)), shape=(6, 6), dtype=np.float32)
        self.chain_csr = self.chain_matrix.tocsr()
        self.chain_csc = self.chain_matrix.tocsc()

    def test_process_row(self):
        shape = self.chain_matrix.shape
        target_matrix = lil_matrix(shape, dtype=np.float32)
        for i in range(6):
            util.process_row(i, target_matrix, (self.chain_csr, self.chain_csc))
        target_matrix = target_matrix.toarray()

        data= np.array([260, 130, 100, 280, 160, 40])
        row = np.array([0, 0, 0, 4, 5, 5])
        col= np.array([2, 3, 5, 1, 2, 3])
        test_results = coo_matrix((data, (row, col)), shape=shape, dtype=np.float32).toarray()

        for i in range(6):
            self.assertTrue(np.array_equal(target_matrix[i], test_results[i]))


if __name__ == '__main__':
    unittest.main()
