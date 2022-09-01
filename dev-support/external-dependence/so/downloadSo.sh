#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# hadolint ignore=DL3003,DL3008
HADOOP_DIST_FOLDER=`cd ../..;pwd`
CURRENT_FOLDER=$(pwd)
SHELL_FOLDER=$(cd "$(dirname "$0")";pwd)

SO_VERSION=$(cd "${HADOOP_DIST_FOLDER}" && mvn help:evaluate -Dexpression=so.version -q -DforceStdout)

cd "${SHELL_FOLDER}"

rm -rf "${SHELL_FOLDER}/hadoop.so*"

echo "SO_VERSION=${SO_VERSION}"

## download hadoop.so
curl -L -s -S "http://nexus-repo.data-infra.shopee.io/repository/maven-release/org/apache/hadoop/hadoop.so/${SO_VERSION}/hadoop.so-${SO_VERSION}.tar.gz" -o "hadoop.so-${SO_VERSION}.tar.gz"

## uncompress
tar -xf "hadoop.so-${SO_VERSION}.tar.gz"

mkdir hadoop-so
mv hadoop-so-${SO_VERSION}/* hadoop-so/

cd "${CURRENT_FOLDER}"
