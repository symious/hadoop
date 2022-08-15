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
CURRENT_FOLDER=$(pwd)
SHELL_FOLDER=$(cd "$(dirname "$0")";pwd)

cd ${SHELL_FOLDER}
rm -rf ${SHELL_FOLDER}/*.jar

ALLUXIO_VERSION='2.7.1-sdi-010'
OZONE_VERSION='1.2.sdi-014'
GPL_COMPRESSION_VERSION='0.1.0'
MC_VERSION='2.2.0'

## download alluxio
curl -L -s -S http://nexus-repo.data-infra.shopee.io/repository/maven-release/org/alluxio/alluxio-shaded-client/${ALLUXIO_VERSION}/alluxio-shaded-client-${ALLUXIO_VERSION}.jar -o alluxio-shaded-client-${ALLUXIO_VERSION}.jar

## download ozone
curl -L -s -S http://nexus-repo.data-infra.shopee.io/repository/maven-release/org/apache/ozone/ozone-filesystem-hadoop3/${OZONE_VERSION}/ozone-filesystem-hadoop3-${OZONE_VERSION}.jar -o ozone-filesystem-hadoop3-${OZONE_VERSION}.jar

## download gpl compression
curl -L -s -S http://nexus-repo.data-infra.shopee.io/repository/maven-mulesoft/com/hadoop/compression/hadoop-gpl-compression/${GPL_COMPRESSION_VERSION}/hadoop-gpl-compression-${GPL_COMPRESSION_VERSION}.jar -o hadoop-gpl-compression-${GPL_COMPRESSION_VERSION}.jar

## download 4mc
curl -L -s -S http://nexus-repo.data-infra.shopee.io/repository/maven-release/com/hadoop/fourmc/hadoop-4mc/${MC_VERSION}/hadoop-4mc-${MC_VERSION}.jar -o hadoop-4mc-${MC_VERSION}.jar

cd ${CURRENT_FOLDER}