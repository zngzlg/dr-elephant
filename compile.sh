#!/usr/bin/env bash

script_dir=`which $0`
script_dir=`dirname $script_dir`
script_dir=`cd $script_dir; pwd`

project_root=$script_dir
start_script=$project_root/start.sh
stop_script=$project_root/stop.sh

cd $project_root

rm -rf dist
mkdir dist

play -Dhadoop.version=1 clean compile test dist

cd target/universal

unzip *.zip

DIST_NAME=$(find * -maxdepth 0 -type d -not -name "tmp")

sed -i.bak $'/declare -r app_classpath/s/.$/:$HADOOP_HOME\/*:$HADOOP_HOME\/lib\/*:$HADOOP_HOME\/conf"\\\naddJava "-Djava.library.path=$HADOOP_HOME\/lib\/native\/Linux-amd64-64"\\\n/'  $DIST_NAME/bin/dr-elephant

chmod +x $DIST_NAME/bin/dr-elephant

cp $start_script $DIST_NAME/

cp $stop_script $DIST_NAME/

zip -r $DIST_NAME-h1.zip $DIST_NAME -x $DIST_NAME/*.zip *.zip tmp/\*  *

mv $DIST_NAME-h1.zip $project_root/dist/

cd $project_root

play -Dhadoop.version=2 clean compile test dist

cd target/universal

unzip *.zip

sed -i.bak $'/declare -r app_classpath/s/.$/:$HADOOP_HOME\/share\/hadoop\/common\/*:$HADOOP_HOME\/share\/hadoop\/common\/lib\/*:$HADOOP_HOME\/share\/hadoop\/hdfs\/*:$HADOOP_CONF_DIR"\\\naddJava "-Djava.library.path=$HADOOP_HOME\/lib\/native"\\\n/'  $DIST_NAME/bin/dr-elephant

chmod +x $DIST_NAME/bin/dr-elephant

cp $start_script $DIST_NAME/

cp $stop_script $DIST_NAME/

zip -r $DIST_NAME-h2.zip $DIST_NAME -x $DIST_NAME/*.zip *.zip tmp/\*  *

mv $DIST_NAME-h2.zip $project_root/dist/
