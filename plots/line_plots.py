import matplotlib.pyplot as plt
import numpy as np

#Example Data
#Different data each time
spark_times1 = [1, 65.993/57.48, 65.993/63.664]  
ray_times1 = [1, 35.541/57.56, 35.541/86.557]  

spark_times2 = [1, 49.07/57.48, 49.07/77.99]  
ray_times2 = [1, 206.732/475.914, 206.732/463.2]

spark_times3 = [1, 135.046/97.487, 135.046/91.63]  
ray_times3 = [1, 356.312/319.581, 356.312/995.66]

spark_sizes = [5/2,10/3,15/4]
ray_sizes = [5/2,10/3,15/4]


#Plots

#First plot
#First line
plt.plot(spark_sizes, spark_times1, marker='o', color='orange', label='Spark', linestyle='-', linewidth=2)

#Second line
plt.plot(ray_sizes, ray_times1, marker='o',  color='dodgerblue', label='Ray', linestyle='-', linewidth=2)



plt.xlabel('Problem Size')
plt.ylabel('Ts/TL')
plt.title('Data Analysis-Query1 Scaleup')
plt.legend()
plt.ylim(0, 3)

#Second plot
fig, ax = plt.subplots()

plt.xlabel('Problem Size')
plt.ylabel('Ts/TL')
plt.title('Data Analysis-Query2 Scaleup')

plt.plot(spark_sizes, spark_times2, marker='o', color='orange', label='Spark', linestyle='-', linewidth=2)
plt.plot(ray_sizes, ray_times2, marker='o',  color='dodgerblue', label='Ray', linestyle='-', linewidth=2)
plt.legend()
plt.ylim(0, 3)

#Third plot
fig, ax = plt.subplots()

plt.xlabel('Problem Size')
plt.ylabel('Ts/TL')
plt.title('Data Analysis-Query3 Scaleup')

plt.plot(spark_sizes, spark_times3, marker='o', color='orange', label='Spark', linestyle='-', linewidth=2)
plt.plot(ray_sizes, ray_times3, marker='o',  color='dodgerblue', label='Ray', linestyle='-', linewidth=2)


plt.legend()
plt.ylim(0, 3)
plt.show()
