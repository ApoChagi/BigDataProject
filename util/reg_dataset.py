from sklearn.datasets import make_regression
import numpy as np
import pandas as pd
import random 

# Adjust samples and features based on required size
samples = 10000000
features = 25

# Generate dataset
X, y = make_regression(n_samples=samples, n_features=features, noise=1, random_state=random.randint(1, 10000))

# Convert to pandas DataFrame
df_X = pd.DataFrame(X, columns=[f'col_{i+1}' for i in range(features)])
df_y = pd.DataFrame(y, columns=['target'])
data = pd.concat([df_X,df_y],axis=1)

# Save as csv
data.to_csv("regression_data0.csv", index=False)

# Display the size in GB
bytes = samples * features * np.dtype(np.float64).itemsize
print(bytes/pow(1024,2))

