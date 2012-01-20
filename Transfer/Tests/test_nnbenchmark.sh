#!/bin/bash

cd /usr/local/hadoop
sudo su hduser -c 'bin/hadoop jar hadoop-*test*.jar nnbench -operation create_write \
    -maps 12 -reduces 6 -blockSize 1 -bytesToWrite 0 -numberOfFiles 1000 \
    -replicationFactorPerFile 3 -readFileAfterOpen true \
    -baseDir /benchmarks/NNBench-`hostname -s`'
sudo su hduser -c 'bin/hadoop dfs -copyToLocal /benchmarks/NNBench-hadooper-master-0/output/_logs/history ~/NNOutput'
cp /usr/local/hadoop/NNBench_results.log ~/Reports/
exit $?
