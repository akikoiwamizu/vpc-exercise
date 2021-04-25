import numpy as np
import time

x = np.ones(1000000000)

# For loop version
t0 = time.time()

total1 = 0

for i in range(len(x)):
    total1 += x[i]

t1 = time.time()

print('For loop run time:', t1 - t0, 'seconds')

# Numpy version
t2 = time.time()

total2 = np.sum(x)

t3 = time.time()

print('Numpy run time:', t3 - t2, 'seconds')
