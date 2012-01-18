#!/bin/bash

cd /usr/local/hadoop
sudo su hduser -c 'bin/hadoop jar hadoop*test*.jar mrbench -numRuns 5'
sudo su hduser -c 'bin/hadoop dfs -copyToLocal /benchmarks/MRBench ~/MRBench'

exit $?
