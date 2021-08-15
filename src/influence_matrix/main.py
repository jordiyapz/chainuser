from src.influence_matrix.util import process_row
import numpy as np
from src.helper import load_matrix, save_matrix

INFLUENCE_FILENAME = 'data/influence_matrix.npz'

if __name__ == '__main__':
    chain_matrix = load_matrix('data/chainmatrix.npz')
    chain_csr = chain_matrix.tocsr()
    chain_csc = chain_matrix.tocsc()

    influence_matrix = load_matrix(INFLUENCE_FILENAME, dtype=np.float32)
    # for i in range(chain_matrix.shape[0]):
        # print(i)
    process_row(4, influence_matrix, (chain_csr, chain_csc), batch_size=1000000)
    # save_matrix(INFLUENCE_FILENAME, influence_matrix)
