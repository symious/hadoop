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

function usage(){
    echo -e "Usage: $0 [-q <yarn queue>] [-i <input_path>] [-o <output_path>]  [-m <map_tasks>]  [-r <reduce_tasks>] "
    exit 1;
}

while getopts ":q:i:o:m:r:" opt; do
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
    *|?|h|:)
        usage;;
  esac
done

hadoop fs -get hdfs://R2/projects/dlm/hdfs/stag/hadoop-shopee-3.3.sdi-conf ${dir}/hadoop-shopee-3.3.sdi-conf

unset HADOOP_HOME
unset HADOOP_CONF_DIR
export HADOOP_HOME=/usr/share/hadoop-3.3
export HADOOP_CONF_DIR=${dir}"/hadoop-shopee-3.3.sdi-conf/etc/hadoop/"

cd ${HADOOP_HOME}
./bin/hadoop jar share/hadoop/tools/lib/hadoop-shopee-3.3.sdi-*.jar \
-D mapreduce.output.fileoutputformat.compress=true \
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.ZStandardCodec \
-D mapreduce.output.fileoutputformat.compress.type=BLOCK \
-D yarn.app.mapreduce.am.staging-dir.erasurecoding.enabled=true \
-D mapreduce.job.queuename=${queue} \
-i ${input_path} \
-o ${output_path} \
-m ${map_tasks} -r ${reduce_tasks}

