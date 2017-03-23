# Spark_WikipediaData
Processes Wikipedia Data to find a list of programming language words and their counts.

Used Spark + Scala on my local machine to do the processing.

### Start and Stop Spark:
  1. from the spark folder do the following start Spark Master and slaves:

      ./sbin/start-master.sh --ip 127.0.0.1 --host 127.0.0.1
      ./sbin/start-slave.sh spark://localhost:7077 --port 7070 --ip localhost

  2. To stop the Spark Cluster
       ./sbin/stop-master.sh 
       ./sbin/stop-slave.sh 
  
  3. Spark Web UI
    http://127.0.0.1:8080/
    
  4. Changes I made in the conf/spark-env.sh file
  
    export SPARK_EXECUTOR_MEMORY=4g
    export SPARK_WORKER_INSTANCES="3"
    export SPARK_WORKER_CORES = 8
    

### This consists of three tasks:
  1. compute the ranking of the languages (`val langs`) by determining the number of Wikipedia articles that
     mention each language at least once and then sort them in decreasing order.
    
  2. Compute Inverted Index for the articles based on the list langs.
  3. Use reduceByKey to rank the languages.
  
### DataSet:

http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat

### Output:

  1. Processing Part 1: naive ranking took 19896 ms.
  2. Processing Part 2: ranking using inverted index took 10993 ms.
  3. Processing Part 3: ranking using reduceByKey took 4496 ms.

