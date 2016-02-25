#!/usr/bin/env bash

#
# Copyright 2016 LinkedIn Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

function print_usage(){
  echo "usage: ./start.sh PATH_TO_CONFIG_FILE(optional)"
}

function check_config(){
  if [ -z "${!1}" ];
  then
    echo "error: ${1} must be present in the config file."
    check=0
  else
    echo "${1}: " ${!1}
  fi
}

# Save project root dir
METRICS_PUBLISHER_CONF_FILE="CounterPublisherConf.xml"
script_dir=`which $0`
script_dir=`dirname $script_dir`
project_root=$script_dir/../

# User could give an optional argument(config file path) or we will provide a default one
if [ -z "$1" ];
then
  echo "Using default config dir: /export/apps/elephant/conf"
  CONF_DIR="/export/apps/elephant/conf"
else
  echo "Using config dir: "$1
  CONF_DIR=$1
fi

CONFIG_FILE=$CONF_DIR"/elephant.conf"
echo "Using config file: "$CONFIG_FILE

# set env variable so Dr. run script will use this dir and load all confs into classpath
export ELEPHANT_CONF_DIR=$CONF_DIR

# User must give a valid file as argument
if [ -f $CONFIG_FILE ];
then
  echo "Reading from config file..."
else
  echo "error: Couldn't find a valid config file at: " $CONFIG_FILE
  print_usage
  exit 1
fi

source $CONFIG_FILE

# db_url, db_name ad db_user must be present in the config file
check=1
check_config db_url
check_config db_name
check_config db_user

if [ $check = 0 ];
then
  echo "error: Failed to get configs for dr.Elephant. Please check the config file."
  exit 1
fi

db_loc="jdbc:mysql://"$db_url"/"$db_name"?characterEncoding=UTF-8"

# db_password is optional. default is ""
db_password="${db_password:-""}"

# keytab_user is optional. defalt is "elephant"
keytab_user="${keytab_user:-elephant}"
echo "keytab_user: " $keytab_user

#keytab_location is optional.
keytab_location="${keytab_location:-/export/apps/hadoop/keytabs/dr_elephant-service.keytab}"
echo "keytab location: " $keytab_location

#port is optional. default is 8080
port="${port:-8080}"
echo "http port: " $port

# Navigate to project root
cd $project_root

# Check if Dr. Elephant already started
if [ -f RUNNING_PID ];
then
  echo "error: Dr. Elephant already started!"
  exit 1
fi

# Dr. Elephant executable not found
if [ ! -f bin/dr-elephant ];
then
  echo "error: I couldn't find any dr. Elephant executable."
  exit 1
fi

# Get hadoop version by executing 'hadoop version' and parse the result
HADOOP_VERSION=$(hadoop version | awk '{if (NR == 1) {print $2;}}')
if [[ $HADOOP_VERSION == 1* ]];
then
  echo "This is hadoop1.x grid. Switch to hadoop2 if you want to use Dr. Elephant"
elif [[ $HADOOP_VERSION == 2* ]];
then
  JAVA_LIB_PATH=$HADOOP_HOME"/lib/native"
  echo "This is hadoop2.x grid. Add Java library path: "$JAVA_LIB_PATH
else
  echo "error: Hadoop isn't properly set on this machine. Could you verify cmd 'hadoop version'? "
  exit 1
fi

OPTS="$jvm_props -Djava.library.path=$JAVA_LIB_PATH -Dhttp.port=$port -Dkeytab.user=$keytab_user -Dkeytab.location=$keytab_location -Ddb.default.url=$db_loc -Ddb.default.user=$db_user -Ddb.default.password=$db_password"

# Start Dr. Elaphant
echo "Starting Dr. Elephant ...."
nohup ./bin/dr-elephant ${OPTS} > /dev/null 2>&1 &

sleep 2

# If Dr. Elephant starts successfully, Play should create a file 'RUNNING_PID' under project root
if [ -f RUNNING_PID ];
then
  echo "Dr. Elephant started."
else
  echo "error: Failed to start Dr. Elephant. Please check if this is a valid dr.E executable or logs under 'logs' directory."
  exit 1
fi
