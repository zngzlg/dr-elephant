#!/usr/bin/env bash

# Navigate to project root dir
script_dir=`which $0`
script_dir=`dirname $script_dir`
project_root=$script_dir/../
cd $project_root

# If file RUNNING_PID exists, it means Dr. Elephant is running
if [ -f RUNNING_PID ];
then
  echo "Dr.Elephant is running."
else
  echo "Dr.Elephant is not running."
  exit 1
fi

# RUNNING_PID contains PID of our Dr. Elephant instance
proc=`cat RUNNING_PID`

echo "Killing Dr.Elephant...."
kill $proc

# Wait for a while
sleep 1

# Play should remove RUNNING_PID when we kill the running process
if [ ! -f RUNNING_PID ];
then
  echo "Dr.Elephant is killed."
else
  echo "Failed to kill Dr.Elephant."
  exit 1
fi
