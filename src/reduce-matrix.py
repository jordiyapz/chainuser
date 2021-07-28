import numpy as np
from scipy.sparse.csc import csc_matrix
from scipy.sparse.csr import csr_matrix
from scipy.sparse.lil import lil_matrix
from src.helper import load_matrix, save_matrix
import pickle

chain_matrix_filename = 'data/chainmatrix.npz'
chain_matrix = load_matrix(chain_matrix_filename)
M = chain_matrix.shape[0]

rows, cols = chain_matrix.nonzero()
rows = np.unique(rows)
cols = np.unique(cols)

survival_candidates = np.asarray(
    [(i in rows or i in cols) for i in range(M)], dtype=bool)

reduced_matrix = csc_matrix(chain_matrix[survival_candidates, :])
reduced_matrix = lil_matrix(reduced_matrix[:, survival_candidates])
indices = [i for i in range(M) if survival_candidates[i]]
save_matrix('data/reducedmatrix.npz', reduced_matrix)

print(survival_candidates.sum())
print(reduced_matrix.todense())
print(np.asarray(indices))
with open('data/reduced-meta.pkl', 'wb') as f:
    pickle.dump(indices, f)
