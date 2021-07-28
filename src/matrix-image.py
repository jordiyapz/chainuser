from src.helper import load_matrix
from PIL import Image
import numpy as np

matrix = load_matrix('data/matrix.npz')
# mat = matrix.todense()
# print(mat.nonzero())
size = matrix.shape[::-1]
databytes = np.packbits(matrix.toarray(), axis=1)
im = Image.frombytes(data=databytes, mode='1', size=size)
im.save('data/matrix.jpg')