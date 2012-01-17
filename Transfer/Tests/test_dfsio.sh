#!/bin/bash

cd /usr/local/hadoop
sudo su hduser -c 'bin/hadoop jar hadoop*test*.jar TestDFSIO -write -nrFiles 10 -fileSize 1000'
sudo su hduser -c 'bin/hadoop jar hadoop*test*.jar TestDFSIO -clean'
sudo su hduser -c 'bin/hadoop jar hadoop*test*.jar TestDFSIO -read -nrFiles 10 -fileSize 1000'
sudo su hduser -c 'bin/hadoop jar hadoop*test*.jar TestDFSIO -clean'
