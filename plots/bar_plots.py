import matplotlib.pyplot as plt
import numpy as np

#Example Data
#Different data each time
spark_times = [65.993, 57.48, 63.66]  
ray_times = [35.541, 57.56, 86.557]   


sets = ['2 nodes-5GB', '3 nodes-10GB', '4 nodes-15GB']



#Set position
bar_width = 0.25
r1 = np.arange(len(sets))
r2 = [x+0.1 + bar_width for x in r1] #add 0.1 to be a little separated

#Creation of the barplot
plt.bar(r1, spark_times, color='orange', width=bar_width, edgecolor='grey', label='Spark')
plt.bar(r2, ray_times, color='dodgerblue', width=bar_width, edgecolor='grey', label='Ray')


plt.ylabel('Time (seconds)')
plt.title('Data analysis-Query1 Latency')
plt.xticks([r + bar_width/2 for r in range(len(sets))], sets)

plt.legend()
plt.ylim(0, 200)

#Label on-top of the bar
for i, value in enumerate(spark_times):
    plt.text(i, value + 0.1, str(value), ha='center', va='bottom', color='black')

for i, value in enumerate(ray_times):
    plt.text(i + bar_width+0.1, value + 0.1, str(value), ha='center', va='bottom', color='black')


plt.show()
