#!/bin/bash

sudo rm -rf Books
mkdir Books
cd Books
wget http://www.gutenberg.lib.md.us/2/0/201/201.txt
wget http://www.gutenberg.lib.md.us/2/0/202/202.txt
wget http://www.gutenberg.lib.md.us/2/0/203/203.txt
wget http://www.gutenberg.lib.md.us/2/0/204/204.txt
wget http://www.gutenberg.lib.md.us/2/0/205/205.txt
wget http://www.gutenberg.lib.md.us/2/0/206/206.txt
wget http://www.gutenberg.lib.md.us/2/0/207/207.txt
wget http://www.gutenberg.lib.md.us/2/0/208/208.txt
wget http://www.gutenberg.lib.md.us/2/0/209/209.txt

cd ..
sudo su hduser -c '/usr/local/hadoop/bin/hadoop dfs -copyFromLocal Books /user/hduser/Books'
sudo rm -rf Books

cd /usr/local/hadoop
sudo su hduser -c 'bin/hadoop jar hadoop*examples*.jar wordcount  /user/hduser/Books /user/hduser/Books_Output'
sudo su hduser -c 'bin/hadoop job -history all /user/hduser/Books_Output' > ~/Reports/wordcdount.results