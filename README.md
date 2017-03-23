# Spark_WikipediaData
Processes Wikipedia Data to find a list of programming language words and their counts.

Used Spark + Scala on my local machine to do the processing.

### This consists of three tasks:
  1. compute the ranking of the languages (`val langs`) by determining the number of Wikipedia articles that
     mention each language at least once and then sort them in decreasing order.
    
  2. Compute Inverted Index for the articles based on the list langs.
  3. Use reduceByKey to rank the languages.
  
### DataSet:

http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat
