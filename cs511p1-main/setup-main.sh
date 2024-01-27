#!/bin/bash
export JAVA_HOME=/usr/local/openjdk-8/jre

####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Setup HDFS/Spark main here
#Adding Hadoop Config files
cat <<-EOL > $HADOOP_HOME/etc/hadoop/core-site.xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://main:9000</value>
    </property>
</configuration>
EOL

cat <<-EOL > $HADOOP_HOME/etc/hadoop/hdfs-site.xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
</configuration>
EOL

#Formatting Namenode(HDFS FS)
$HADOOP_HOME/bin/hdfs namenode -format

# Adding Spark Config Files
cat <<EOL > $SPARK_HOME/conf/spark-defaults.conf
<configuration>
    <property>
        <name>spark.master</name>
        <value>spark://main:7077</value>
    </property>
</configuration>
EOL
