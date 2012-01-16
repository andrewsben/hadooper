#!/bin/bash



sudo mv /usr/bin/ssh /usr/bin/ssh-orig
sudo su -c "echo '#
ssh-orig -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no \"\$@\"
' > /usr/bin/ssh"
sudo chmod 777 /usr/bin/ssh

cat << EOD | sudo debconf-set-selections
sun-java5-jdk shared/accepted-sun-dlj-v1-1 select true
sun-java5-jre shared/accepted-sun-dlj-v1-1 select true
sun-java6-jdk shared/accepted-sun-dlj-v1-1 select true
sun-java6-jre shared/accepted-sun-dlj-v1-1 select true
EOD

sudo dpkg --set-selections << EOS
sun-java6-jdk install
EOS


sudo add-apt-repository "deb http://archive.canonical.com/ubuntu natty partner"
sudo apt-get update


sudo apt-get install -y sun-java6-jdk
sudo update-java-alternatives -s java-6-sun


sudo addgroup hadoop
sudo useradd -d /home/hduser -m -g hadoop -s /bin/bash hduser
sudo mkdir /home/hduser/.ssh
sudo touch /home/hduser/.ssh/authorized_keys
sudo chown ubuntu:ubuntu /home/hduser/.ssh/authorized_keys
cat Transfer/ssh_key.pub > /home/hduser/.ssh/authorized_keys
sudo cp /home/ubuntu/Transfer/ssh_key /home/hduser/.ssh/id_rsa
cp /home/ubuntu/Transfer/ssh_key /home/ubuntu/.ssh/id_rsa
sudo chown -R hduser:hadoop /home/hduser




cd ~/Transfer
sudo tar xzf hadoop-1.0.0.tar.gz
sudo mv hadoop-1.0.0 /usr/local/hadoop
sudo cp slaves /usr/local/hadoop/conf/
sudo cp masters /usr/local/hadoop/conf/
sudo chown -R hduser:hadoop /usr/local/hadoop

sudo chown ubuntu /home/hduser/.bashrc
cat bashrc_add >> /home/hduser/.bashrc
sudo chown -R hduser:hadoop /home/hduser
sudo cp *.xml /usr/local/hadoop/conf/
sudo cp hadoop-env.sh /usr/local/hadoop/conf/

sudo mkdir -p /app/hadoop/tmp
sudo chown hduser:hadoop /app/hadoop/tmp

sudo su -c "cat /home/ubuntu/Transfer/add_to_hosts >> /etc/hosts"

sudo mv /usr/bin/ssh-orig /usr/bin/ssh 
exit $?
