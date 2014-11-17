#!/usr/bin/env bash

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
  echo "Using default config file: /export/apps/elephant/conf/elephant.conf"
  CONFIG_FILE="/export/apps/elephant/conf/elephant.conf"
else
  CONFIG_FILE=$1
fi

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
  JAVA_LIB_PATH=$HADOOP_HOME"/lib/native/Linux-amd64-64"
  echo "This is hadoop1.x grid. Add Java library path: "$JAVA_LIB_PATH
elif [[ $HADOOP_VERSION == 2* ]];
then
  JAVA_LIB_PATH=$HADOOP_HOME"/lib/native"
  echo "This is hadoop2.x grid. Add Java library path: "$JAVA_LIB_PATH
else
  echo "error: Hadoop isn't properly set on this machine. Could you verify cmd 'hadoop version'? "
  exit 1
fi

OPTS="-Djava.library.path=$JAVA_LIB_PATH -Dhttp.port=$port -Dkeytab.user=$keytab_user -Dkeytab.location=$keytab_location -Ddb.default.url=$db_loc -Ddb.default.user=$db_user -Ddb.default.password=$db_password"

CONFIG_DIR=`dirname ${CONFIG_FILE}`
METRICS_PUBLISHER_CONF_PATH="${CONFIG_DIR}/${METRICS_PUBLISHER_CONF_FILE}"
if [ -r ${METRICS_PUBLISHER_CONF_PATH} ]; then
  OPTS="${OPTS} -Dmetrics.publisher-conf=${METRICS_PUBLISHER_CONF_PATH}"
fi

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
