# Instructions on how to run the scripts on a local Ray Cluster

The first step in order to run the scripts is to setup the Ray Cluster:
First of all, we need to declare the `CLASSPATH` environment variable by using  
``` 
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
```
Since our scripts read from HDFS, we need to start HDFS and YARN:
```
start-dfs.sh
start-yarn.sh
```
### Python Dependencies
Install the required dependencies for all scripts:
```
pip install pyarrow pandas xgboost_ray transformers
```
### 1. Start the head node by running the following command:
``` 
ray start --head --port 6397
 ```
### 2. Add the other nodes to the cluster:
``` 
ray start --address='<HEAD_NODE_IP>:6397'
```
Make sure to change the <HEAD_NODE_IP> to the actual head node's IPv4 address
### 3. Submit the job :
Submit the script from head node by using the ` ray job sumbit `: <br>
``` 
ray job submit python3 /path/to/script.py
 ``` 
Replace /path/to/script.py with the actual (full path) of the script. Alternative, you can run the script from any node by simply
running  <br> ` python3 script.py ` <br> We recommend using the **ray job submit** in order to be able to monitor the job from Ray Dashboard, which is available <b> on head node </b> at `http://localhost:8265`

### Configure the scripts:
You can manually configure the ray parameters (such as Actors, cpu_per_actor, etc) to match the cluster resources.
For xgboost configuration on Ray Cluster, refer to the [official documentation](https://xgboost.readthedocs.io/en/stable/tutorials/ray.html)
