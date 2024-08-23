'''
  Distributed XGBoost Random Forest Regressor training and prediction on Spark
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler 
from xgboost.spark import SparkXGBRegressor
import sys
import time


spark = SparkSession.builder\
                    .appName("XGBOOST_regression_spark")\
                    .getOrCreate()


# Redirect output to corresponding file
sys.stdout = open("XGBOOST_regression_spark.out", "w")

'''
  Load csv files from HDFS directory intto a Spark DataFrame
  and cast all fields to Double

  The number of csv files under this directory changed
  during development in order to test the scalability
'''
dataset_path = 'hdfs://gutasVM:54310/regression_data/'
start = time.time()
df = spark.read.csv(dataset_path, header=True, inferSchema=True)
df = df.select([col(c).cast("Double") for c in df.columns])

# Split features and target
features_col = df.columns[:-1]
featureassembler = VectorAssembler(inputCols=features_col, outputCol="features")
finalized_data = featureassembler.transform(df).select("features", "target")

# Test train split
train_data,test_data=finalized_data.randomSplit([0.75,0.25])

# Regressor model
regressor = SparkXGBRegressor(
  features_col="features",
  label_col="target",
  num_workers=2,
  booster="gbtree",
  device = "cpu", missing=0.0, 
  eval_metric='rmse')


# train and return the model
model = regressor.fit(train_data)

# predict on test data
predict_df = model.transform(test_data)
x = predict_df.collect() # action to trigger results

end = time.time()


print("Total time: ", (end-start))
