# Big-Data-Processing-Example3 : Spark Streaming Project

Implemented Scala based Maven Project to monitor a folder in HDFS in real time such that any new file in the folder is processed at real time.

For each RDD (Resilient Distributed Dataset) in the stream, below tasks are performed: 

1. Count the Word frequency by trimming all non-alpha-numeric characters from the text and save the output into HDFS.
2. Filter out short words (length < 5) and save them as a result into HDFS. 
3. Count co-occurence of words in each RDD where context is a same line and save the output into HDFS.
