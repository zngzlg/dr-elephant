#!/usr/bin/env bash

set -x
trap "exit" SIGINT SIGTERM

project_root=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${project_root}

start_script=${project_root}/start.sh
stop_script=${project_root}/stop.sh

# Echo the value of pwd in the script so that it is clear what is being removed.
rm -rf ${project_root}/dist
mkdir dist

play -Dhadoop.version=1 clean compile test dist

cd target/universal

# TODO Good to add a check for exactly one zip here
ZIP_NAME=`/bin/ls *.zip`
unzip ${ZIP_NAME}
DIST_NAME=${ZIP_NAME%.zip}

# Make two changes to bin/dr-elephant file
# Add :$HADOOP_HOME/*:$HADOOP_HOME/lib/*:$HADOOP_HOME/conf to 'declare -r ...' line
# Add a line addJava "-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64" right after
sed -i.bak $'/declare -r app_classpath/s/.$/:$HADOOP_HOME\/*:$HADOOP_HOME\/lib\/*:$HADOOP_HOME\/conf"\\\naddJava "-Djava.library.path=$HADOOP_HOME\/lib\/native\/Linux-amd64-64"\\\n/'  ${DIST_NAME}/bin/dr-elephant

chmod +x ${DIST_NAME}/bin/dr-elephant

cp $start_script ${DIST_NAME}/bin/

cp $stop_script ${DIST_NAME}/bin/

zip -r ${DIST_NAME}-h1.zip ${DIST_NAME} -x "${DIST_NAME}/*.zip"

mv ${DIST_NAME}-h1.zip ${project_root}/dist/

cd ${project_root}

play -Dhadoop.version=2 clean compile test dist

cd target/universal

unzip *.zip

sed -i.bak $'/declare -r app_classpath/s/.$/:$HADOOP_HOME\/share\/hadoop\/common\/*:$HADOOP_HOME\/share\/hadoop\/common\/lib\/*:$HADOOP_HOME\/share\/hadoop\/hdfs\/*:$HADOOP_CONF_DIR"\\\naddJava "-Djava.library.path=$HADOOP_HOME\/lib\/native"\\\n/'  ${DIST_NAME}/bin/dr-elephant

chmod +x ${DIST_NAME}/bin/dr-elephant

cp $start_script ${DIST_NAME}/bin

cp $stop_script ${DIST_NAME}/bin

zip -r ${DIST_NAME}-h2.zip ${DIST_NAME} -x "${DIST_NAME}/*.zip"

mv ${DIST_NAME}-h2.zip $project_root/dist/
