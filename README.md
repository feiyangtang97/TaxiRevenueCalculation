## Exercise 1:

#### Command for Exercise 1 Pyspark Python script:
```
python main.py ../data/2010_03.trips
```

#### Command for Exercise 1 Java with Spark script:
```
spark-submit --class "Main" --master "local[2]"  Exercise1.jar ../data/2010_03.trips
```


## Exercise 2:

#### Command for the most efficient solution:
```
hadoop jar  Exercise2.jar Main -D mapred.reduce.tasks=39 -D mapred.min.split.size=359496729 -D mapreduce.job.reduce.slowstart.completedmaps=0.2 -D mapreduce.map.cpu.vcores=4 -D mapreduce.reduce.cpu.vcores=4 -D mapreduce.map.memory.mb=512 -D mapreduce.reduce.memory.mb=768 <data> <trips dir> <revenue dir> <revenue aggregate type>
```

#### Sample command:
```
hadoop jar Exercise2.jar Main -D mapred.reduce.tasks=39 -D mapred.min.split.size=359496729 -D mapreduce.job.reduce.slowstart.completedmaps=0.2 -D mapreduce.map.cpu.vcores=4 -D mapreduce.reduce.cpu.vcores=4 -D mapreduce.map.memory.mb=512 -D mapreduce.reduce.memory.mb=768 /data/all.segments /user/r0728168/bdap/trip /user/r0728168/bdap/rev month
```

## Remarks:
revenue aggregate type can be "day" or "month", "day" means to calculate the revenue by day, "month" means to calculate the revenue on a monthly basis.
