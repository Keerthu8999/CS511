#!/bin/bash
export JAVA_HOME=/usr/local/openjdk-8/jre

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Setup HDFS/Spark worker here
#Adding Hadoop Config files
cat <<EOL > $HADOOP_HOME/etc/hadoop/core-site.xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://main:9000</value>
    </property>
</configuration>
EOL

#Adding Spark Config files
cat <<EOL > $SPARK_HOME/conf/spark-defaults.conf
<configuration>
    <property>
        <name>spark.master</name>
        <value>spark://main:7077</value>
    </property>
</configuration>
EOL