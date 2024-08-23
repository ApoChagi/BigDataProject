'''
    Distributed XGBoost Random Forest Regressor training and prediction on Ray
'''
import ray
from pyarrow import csv
import sys
from xgboost_ray import RayDMatrix, RayParams, train, RayXGBRFRegressor
import time

# Connect to existing local cluster
ray.init(address='auto')

# Redirect stdout, stderr to corresponding files 
sys.stdout = open("xgb_regression_ray.out", "w")

start = time.time()

'''
    Load csv files from HDFS directory to a Ray Dataset

    The number of csv files under this directory changed
    during development in order to test the scalability
'''
dataset_path = 'hdfs://gutasVM:54310/regression_data/'
ds = ray.data.read_csv(dataset_path, parallelism=-1)

# Dataset split
X_train, X_test = ds.train_test_split(test_size=0.25)
train_set = RayDMatrix(X_train, label='target') 

# Regressor model
reg = RayXGBRFRegressor(
    booster='gbtree',
    enable_categorical=True,
)


'''
Distributed training

Parameters `num_actors` and `cpus_per_actor` were adjusted before
each training session to comply with the available cluster resources

Refer to https://xgboost.readthedocs.io/en/stable/tutorials/ray.html 
for more info about XGBoost parameters on ray

The following is an example for a 2-node cluster with 4 CPU cores each 

'''
reg.fit(train_set, None, ray_params=RayParams(  num_actors=2,         \
                                                elastic_training=True,\
                                                max_failed_actors=2,  \
                                                max_actor_restarts=3, \
                                                cpus_per_actor=4,     \
                                                gpus_per_actor=0 \
                                            ))

# Distributed prediction
scored = X_test.drop_columns(["target"]) \
               .map_batches( lambda batch: {"pred": reg.predict(RayDMatrix(batch))}, batch_format="pandas")

scored.show()

end = time.time()

print("Total time: ", (end-start)) 
