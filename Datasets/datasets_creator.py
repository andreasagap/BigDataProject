

import numpy as np
import matplotlib.pyplot as plt
import random


def generate_uniform(params_min,params_max,n):
    uniform = np.random.uniform(low=params_min,high=params_max,size=(1000,n))
    plt.scatter(uniform[:, 0], uniform[:, 1], c='green')
    plt.show()

    f = open("uniform"+str(n)+".txt", "a")
    for element in uniform:
        string_ints = [str(int) for int in element]
        f.write(",".join(string_ints)+"\n")
    f.close()

if __name__ == '__main__':
    generate_uniform([0.0,0.0],[1.0,1.0],2)
    generate_uniform([0.0, 0.0,0.0], [1.0, 1.0, 1.0], 3)
    generate_uniform([0.0, 0.0,0.0,0.0,0.0], [1.0, 1.0, 1.0,1.0, 1.0], 5)
