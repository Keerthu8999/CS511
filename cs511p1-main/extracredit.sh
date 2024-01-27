#!/bin/bash
#Main Container ID
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

MAIN_CONTAINER_ID=$(docker-compose -f cs511p1-compose.yaml ps -q main)
echo "Removing Temporary files from HDFS as it could skew the bash grading - Ignore cannot remove No such file or directory message on first run"
docker-compose -f cs511p1-compose.yaml exec main bash -x -c 'rm /TeraSortingCap.csv'
docker-compose -f cs511p1-compose.yaml exec main bash -x -c 'rm /TeraSortingCap.scala'
docker-compose -f cs511p1-compose.yaml exec main bash -x -c 'rm /sortedterasortingcap.csv'
echo -e "\n"
# Copying files
docker cp ./TeraSortingCap.scala $MAIN_CONTAINER_ID:/TeraSortingCap.scala
docker cp ./TeraSortingCap.csv $MAIN_CONTAINER_ID:/TeraSortingCap.csv

#Spark Component
# Compiling SBT 
#https://docs.scala-lang.org/getting-started/sbt-track/getting-started-with-scala-and-sbt-on-the-command-line.html
#https://www.scala-sbt.org/1.x/docs/Directories.html
#https://medium.com/@wangyunlongau/scala-sbt-project-directory-structure-c254bb08623e
docker-compose -f cs511p1-compose.yaml exec main bash -x -c '
    mkdir -p ~/TeraSortingCap/src/main/scala; \
    cp /TeraSortingCap.scala ~/TeraSortingCap/src/main/scala/; \
    echo "name := \"TeraSortingCap\"" > ~/TeraSortingCap/build.sbt; \
    echo "version := \"1.0\"" >> ~/TeraSortingCap/build.sbt; \
    echo "scalaVersion := \"2.12.18\"" >> ~/TeraSortingCap/build.sbt; \
    echo "libraryDependencies += \"org.apache.spark\" %% \"spark-core\" % \"3.4.1\"" >> ~/TeraSortingCap/build.sbt; \
    echo "libraryDependencies += \"org.apache.spark\" %% \"spark-sql\" % \"3.4.1\"" >> ~/TeraSortingCap/build.sbt; \
    cd ~/TeraSortingCap; \
    sbt compile; \
    sbt package'
# Uploading csv to HDFS
docker-compose -f cs511p1-compose.yaml exec main bash -x -c '
    hdfs dfs -mkdir -p /data; \
    hdfs dfs -put -f /TeraSortingCap.csv /data/TeraSortingCap.csv'
#docker-compose -f cs511p1-compose.yaml exec main bash -x -c 'cat /TeraSortingCap.csv'
# Start Spark Job
docker-compose -f cs511p1-compose.yaml exec main bash -x -c '
    spark-submit --class TeraSortingCap --master spark://main:7077 ~/TeraSortingCap/target/scala-2.12/terasortingcap_2.12-1.0.jar'
# Fetching sorted csv file
docker-compose -f cs511p1-compose.yaml exec main bash -x -c '
    hdfs dfs -get /data/sortedterasortingcap/part-* /sortedterasortingcap.csv'
#Bringing file to local filesystem
docker cp $MAIN_CONTAINER_ID:/sortedterasortingcap.csv ./sortedterasortingcap.csv
echo -e "\n"
echo "Original File"
cat ./TeraSortingCap.csv
echo -e "\n"
echo "Sorted & Filtered CSV File from Spark"
cat ./sortedterasortingcap.csv

#Bash Grading 
echo -e "\n"
echo "Grading it by sorting in bash"
# Extracting Header to prepare for bash sort
head -n 1 ./TeraSortingCap.csv > ./BashTeraSortingCapHeader.csv
# Sorting the body in Bash
awk -F, 'NR > 1 && $1 <= 2023' ./TeraSortingCap.csv | sort -t, -k1,1nr -k2,2 > ./BashTeraSortingCapBody.csv
cat ./BashTeraSortingCapBody.csv
# Merging Sorted csv from bash with the Header file
cat ./BashTeraSortingCapHeader.csv ./BashTeraSortingCapBody.csv > ./grading.csv
# Compare the two files
diff ./sortedterasortingcap.csv ./grading.csv
if [ $? -eq 0 ]; then
    echo -e " ${GREEN}PASS - Spark sort is same as Bash Grader!${NC}"
else
    echo -e " ${RED}FAIL Output is different, Review the files,find the relevant row${NC}"
fi

#Clean up local files
rm ./grading.csv ./BashTeraSortingCapHeader.csv ./BashTeraSortingCapBody.csv
echo -e "\n"
echo -e " ${NC}The file sortedterasortingcap.csv generated is the sorted csv placed in ./cs511p1/ directory alongisde the source csv file and all other csv i.e the bash ones are cleaned up now${NC}"
