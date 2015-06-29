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

play clean test compile dist

cd target/universal

ZIP_NAME=`/bin/ls *.zip`
unzip ${ZIP_NAME}
rm ${ZIP_NAME}
DIST_NAME=${ZIP_NAME%.zip}

chmod +x ${DIST_NAME}/bin/dr-elephant

sed -i.bak $'/declare -r app_classpath/s/.$/:`hadoop classpath`:${ELEPHANT_CONF_DIR}"/' ${DIST_NAME}/bin/dr-elephant

cp $start_script ${DIST_NAME}/bin/

cp $stop_script ${DIST_NAME}/bin/

zip -r ${DIST_NAME}.zip ${DIST_NAME}

mv ${DIST_NAME}.zip ${project_root}/dist/
