#!/usr/bin/env bash
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

#execute ec file verify.

set -e

MYNAME="${BASH_SOURCE-$0}"
echo $MYNAME
dir=$(cd -P -- "$(dirname -- "${MYNAME}")" >/dev/null && pwd -P)
cd $dir

function log_info () {
  DATE=`date "+%Y-%m-%d %H:%M:%S"`
  echo "${DATE}[INFO] $@"
}

function usage(){
    echo -e "Usage: $0 [-q <yarn queue>] [-i <input_path>] [-o <output_path>]  [-m <map_tasks>]  [-r <reduce_tasks>] [-d <hive db name>]  [-t <hive table name>] "
    exit 1;
}

while getopts ":q:i:o:m:r:d:t:" opt; do
  case $opt in
    q)
        queue=${OPTARG};;
    i)
        input_path=${OPTARG};;
        #default online specify is hdfs://R2/projects/dlm/hdfs/stag/inputec
    o)
        output_path=${OPTARG};;
        #default online specify is hdfs://R2/projects/dlm/hive/dlm/ec_file_validate_result
    m)
        map_tasks=${OPTARG};;
    r)
        reduce_tasks=${OPTARG};;
    d)
        db=${OPTARG};;
    t)
        table=${OPTARG};;
    *|?|h|:)
        usage;;
  esac
done

log_info "start execute ec file validate."

log_info "start execute select ec file job."
grass_date="$(date -d -1day +%Y-%m-%d)"
spark-submit \
  --deploy-mode cluster \
  --executor-memory 10G \
  --executor-cores 3 \
  --queue ${queue} \
  select_ec_file.py hdfs://R2/projects/dlm/hdfs/stag/inputec $grass_date

if [ $? -ne 0 ]; then
    log_info "execute select ec file job failed"
    exit 1
else
    log_info "execute select ec file job succeed"
fi

log_info "start execute validate ec file job."
sh ec_verify_mr.sh -q ${queue} -i hdfs://R2/projects/dlm/hdfs/stag/inputec/$grass_date \
-o hdfs://R2/projects/dlm/hive/dlm/ec_file_validate_result/$grass_date \
-m 100 -r 100

if [ $? -ne 0 ]; then
    log_info "execute validate ec file job failed"
    exit 1
else
    log_info "execute validate ec file job succeed"
fi

log_info "start execute load ec result to hive table."

spark-submit \
  --deploy-mode cluster \
  --executor-memory 10G \
  --executor-cores 3 \
  --queue ${queue} \
  load_ec_file_validate_result.py hdfs://R2/projects/dlm/hive/dlm/ec_file_validate_result $grass_date $db $table

if [ $? -ne 0 ]; then
    log_info "execute load ec result to hive table failed"
    exit 1
else
    log_info "execute load ec result to hive table succeed"
fi

log_info "complete execute ec file validate."
exit 0
