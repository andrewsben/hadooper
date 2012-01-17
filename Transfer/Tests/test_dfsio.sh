#!/bin/bash


cd /usr/local/hadoop
sudo su hduser -c 'bin/hadoop jar hadoop*test*.jar TestDFSIO -write -nrFiles 5 -fileSize 100'
sudo su hduser -c 'bin/hadoop jar hadoop*test*.jar TestDFSIO -read -nrFiles 5 -fileSize 100'
sudo su hduser -c 'bin/hadoop jar hadoop*test*.jar TestDFSIO -clean'

exit $?
