#!/bin/bash
sudo mkdir -p /home/hadoop
sudo mkdir -p /usr/lib/spark/jars
sudo mkdir -p /usr/lib/hadoop/lib
aws s3 cp s3://colaberrycodefilelocationforemr/mysql-connector-java-8.0.27.jar /home/hadoop
chmod 777 /home/hadoop/mysql-connector-java-8.0.27.jar
sudo cp /home/hadoop/mysql-connector-java-8.0.27.jar /usr/lib/spark/jars
sudo cp /home/hadoop/mysql-connector-java-8.0.27.jar /usr/lib/hadoop/lib
