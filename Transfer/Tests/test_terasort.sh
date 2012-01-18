#!/bin/bash

cd /usr/local/hadoop

sudo su hduser -c 'bin/hadoop jar hadoop*examples*.jar teragen -D dfs.block.size=536870912 1000000 /user/hduser/terasort-input'
sudo su hduser -c 'bin/hadoop jar hadoop-*examples*.jar terasort /user/hduser/terasort-input /user/hduser/terasort-output'
sudo su hduser -c 'bin/hadoop jar hadoop*examples*.jar teravalidate /user/hduser/terasort-output /user/hduser/terasort-validate'
sudo su hduser -c 'bin/hadoop job -history all /user/hduser/terasort-input' > ~/Reports/teragen.results
sudo su hduser -c 'bin/hadoop job -history all /user/hduser/terasort-output' > ~/Reports/terasort.results
sudo su hduser -c 'bin/hadoop job -history all /user/hduser/terasort-validate' > ~/Reports/teravalidate.results

exit $?
