#!/bin/bash

# WordCount2 Run simlely:
hadoop jar target/hadoop-basic-1.0-SNAPSHOT.jar WordCount2 /user/joe/wordcount/input /user/joe/wordcount/output

# WordCount2 Run with pattern
# hadoop jar target/hadoop-basic-1.0-SNAPSHOT.jar WordCount2 -Dwordcount.case.sensitive=false /user/joe/wordcount/input /user/joe/wordcount/output -skip /user/joe/wordcount/patterns.txt