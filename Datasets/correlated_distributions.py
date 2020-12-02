import numpy as np
import matplotlib.pyplot as plt


def scaling(x, y):

    for i in range(0, len(y)):
        y[i] = (y[i] - np.min(y)) / (np.max(y) - np.min(y))
        x[i] = (x[i] - np.min(x)) / (np.max(x) - np.min(x))

    return x, y


mean = [0, 0]

# anti - correlated
#cov = [[1, -0.9], [-0.9, 1]]

# correlated
cov = [[1, 0.7], [0.7, 1]]

# 3 dimensions
# cov = [[1, -0.8, 0.9], [-0.8, 1, 0.1], [0.9, 0.1, 1]]

x, y = np.random.multivariate_normal(mean, cov, 5000).T
# x, y, z

x, y = scaling(x, y)

plt.scatter(x, y)
plt.show()
