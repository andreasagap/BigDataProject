import numpy as np
import matplotlib.pyplot as plt
import random


def create_covariance_matrix(n, d):

    if d == 'normal':
        array = np.zeros((n, n))
    if d == 'correlated':
        array = np.random.uniform(low=0.8, high=1.0, size=(n, n))
    if d == 'anticorrelated':
        # array = np.random.rand(n, n) * 2 - 1
        array = np.random.uniform(low=0, high=0.25, size=(n, n))

    array = np.round(array, decimals=2)
    symmetric_array = (array + array.T) / 2

    if d == 'anticorrelated':

        symmetric_array[0][1] = -0.9
        symmetric_array[1][0] = -0.9

    np.fill_diagonal(symmetric_array, 1)

    print(symmetric_array)
    return symmetric_array


def generate_distribution(n, d, nsamples):

    if d != 'uniform':
        cov = create_covariance_matrix(n, d)
        mean = [0] * n
        distribution_array = np.random.multivariate_normal(mean, cov, nsamples).T
    else:
        lower = 0
        upper = 1
        distribution_array = np.random.uniform(low=lower, high=upper, size=(nsamples, n), ).T

    scaled_array = scaling(distribution_array)
    rounded_array = np.round(scaled_array, decimals=2)

    return rounded_array


def scaling(arr):
    for j in range(0, len(arr)):
        col = arr[j]
        for i in range(0, len(col)):
            arr[j][i] = (arr[j][i] - np.min(arr[j])) / (np.max(arr[j]) - np.min(arr[j]))

    return arr


def plot_2d_scatter(arr):
    plt.scatter(arr[0], arr[1])
    plt.show()


def write_array_to_txt(arr, fname):
    with open(fname, 'wb') as f:
        np.savetxt(f, np.column_stack(arr), fmt='%.2f')


if __name__ == '__main__':
    d_type = 'uniform'
    dim = 3
    n_samples = 5000

    distribution = generate_distribution(dim, d_type, n_samples)

    plot_2d_scatter(distribution)

    f_name = d_type + '_dim_' + str(dim) + '_nsamples_' + str(n_samples) + '.txt'
    write_array_to_txt(distribution, f_name)

