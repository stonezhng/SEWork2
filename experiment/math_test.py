from matplotlib import pyplot as plt
import numpy as np

x = np.arange(0, 27, 1)
print x
y = (28 - x) * 1.75 + 49
plt.plot(x, y)
plt.show()
