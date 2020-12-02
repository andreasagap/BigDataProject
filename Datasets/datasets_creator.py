
import numpy as np
import matplotlib.pyplot as plt
import random
from mpl_toolkits import mplot3d
def scaling(x, y):

    for i in range(0, len(y)):
        y[i] = (y[i] - np.min(y)) / (np.max(y) - np.min(y))
        x[i] = (x[i] - np.min(x)) / (np.max(x) - np.min(x))

    return x, y
def scaling(x, y,z):

    for i in range(0, len(y)):
        y[i] = (y[i] - np.min(y)) / (np.max(y) - np.min(y))
        x[i] = (x[i] - np.min(x)) / (np.max(x) - np.min(x))
        z[i] = (z[i] - np.min(z)) / (np.max(z) - np.min(z))
    return x, y,z

def generate_correlated(n):
    if n == 2:
        mean = [0, 0]
        cov = [[1, 0.7],
               [0.7, 1]]
        x, y = np.random.multivariate_normal(mean, cov, 1000).T
        x, y = scaling(x, y)
        f = open("correlated" + str(n) + ".txt", "a")
        for x1, y1 in zip(x, y):
            f.write(str(x1) + "," + str(y1) + "\n")
        f.close()
    elif n == 3:
        mean = [0, 0,0]
        cov = [[1., -0.8, -0.8],
               [-0.8, 1., 0.9],
               [-0.8, 0.9, 1.]]
        x, y,z = np.random.multivariate_normal(mean, cov, 1000).T
        x, y,z = scaling(x, y,z)
        f = open("correlated" + str(n) + ".txt", "a")
        for x1, y1,z1 in zip(x, y,z):
            f.write(str(x1) + "," + str(y1) + "," + str(z1) + "\n")
        f.close()
        fig = plt.figure()
        ax = plt.axes(projection='3d')
        ax.scatter3D(x, y, z, c=z, cmap='Greens')
        plt.show()
    elif n == 5:
        print("AFTER")




def generate_uniform(params_min, params_max, n):
    uniform = np.random.uniform(low=params_min, high=params_max, size=(1000, n))
    plt.scatter(uniform[:, 0], uniform[:, 1], c='green')
    plt.show()

    f = open("uniform" + str(n) + ".txt", "a")
    for element in uniform:
        string_ints = [str(int) for int in element]
        f.write(",".join(string_ints) + "\n")
    f.close()


if __name__ == '__main__':
    # generate_uniform([0.0, 0.0], [1.0, 1.0], 2)
    # generate_uniform([0.0, 0.0, 0.0], [1.0, 1.0, 1.0], 3)
    # generate_uniform([0.0, 0.0, 0.0, 0.0, 0.0], [1.0, 1.0, 1.0, 1.0, 1.0], 5)
    generate_correlated(3)
