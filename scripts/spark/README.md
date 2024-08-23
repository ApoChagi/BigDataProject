# Instructions on how to run the Spark scripts

## 1. Make sure that HDFS and YARN are running
You can start the HDFS and YARN by using the following commands on NameNode:
```
start-dfs.sh
start-yarn.sh
```

## 2. Submit the job

### 2.1 Run dataset_analysis_spark.py 
To run the scripts with default properties (the configuration in `$SPARK_HOME/conf/spark-defaults.conf`) you can use:
```
spark-submit script.py
```
You can also explicitly specify spark parameters such as number of executors or executor memory :
```
spark-submit \
--num-executors 4 \
--conf spark.executor.memory='2g' \
script.py
```
### 2.2 Run xgb_regression_spark.py
This script requires some dependecies (python modules) to be installed on all nodes. We recommend using a virtual environment
with python's [venv](https://docs.python.org/3/library/venv.html).
To create the virtual environment you can run the following commands:
```
python -m venv xgboost_env
source xgboost_env/bin/activate
pip install pyarrow pandas venv-pack xgboost
venv-pack -o xgboost_env.tar.gz
```
The next step is to set the following two environment variables:
```
export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_PYTHON=./environment/bin/python3
```
Finally, you can submit the application using spark-submit by specifying the **--archives xgboost_env.tar.gz#environment** : 
```
spark-submit \
  --num-executors 10 \
` --conf spark.executor.memory='2g' \
  --archives xgboost_env.tar.gz#environment \
  xgb_regression_spark.py
```

### 2.3 Run bert_spark.py
Another virtual environment is recommended for this script. Full instructions below:
```
python -m venv bert_env
source bert_env/bin/activate
pip install transformers pyarrow
venv-pack -o bert_env.tar.gz
export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_PYTHON=./environment/bin/python3
# Adjust the following spark parameters
spark-submit \
  --num-executors 10 \
  --archives bert_env.tar.gz#environment \
  bert_spark.py
```
### Monitor the applications
You can monitor the application's progress both on YARN's UI and on SparkHistory server <br>

#### YARN UI
YARN UI is available on `http://<namenode_ip>:8088/cluster` <br>
#### SparkHistory Server
You can start the spark history server with : 
```
$SPARK_HOME/sbin/start-history-server.sh
```
The server is available on `http://<namenode_ip>:18080` <br>
*Replace <name_node_ip> with actual NameNode's IPv4 address*
