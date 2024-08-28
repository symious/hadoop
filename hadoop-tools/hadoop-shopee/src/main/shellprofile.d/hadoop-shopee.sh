#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if ! declare -f hadoop_subcommand_ecvalidator >/dev/null 2>/dev/null; then

  if [[ "${HADOOP_SHELL_EXECNAME}" = hadoop ]]; then
    hadoop_add_subcommand "ecvalidator" client "run a map-reduce job validate ec file"
  fi

  # this can't be indented otherwise shelldocs won't get it

## @description  ecvalidator command for hadoop
## @audience     public
## @stability    stable
## @replaceable  yes
function hadoop_subcommand_ecvalidator
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.ec.ECValidatorJob
  hadoop_add_to_classpath_tools hadoop-shopee
}

fi


if ! declare -f hadoop_subcommand_compatibilitytest >/dev/null 2>/dev/null; then

  if [[ "${HADOOP_SHELL_EXECNAME}" = hadoop ]]; then
    hadoop_add_subcommand "compatibilitytest" client "run a compatibility test between different hadoop versions"
  fi

  # this can't be indented otherwise shelldocs won't get it

## @description  compatibilitytest command for hadoop
## @audience     public
## @stability    stable
## @replaceable  yes
function hadoop_subcommand_compatibilitytest
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.compatibility.TestCase
  hadoop_add_to_classpath_tools hadoop-shopee
}

fi

if ! declare -f mapred_subcommand_ecvalidator >/dev/null 2>/dev/null; then

  if [[ "${HADOOP_SHELL_EXECNAME}" = mapred ]]; then
    hadoop_add_subcommand "ecvalidator" client "run a map-reduce job validate ec file"
  fi

  # this can't be indented otherwise shelldocs won't get it

## @description  ecvalidator command
## @audience     public
## @stability    stable
## @replaceable  yes
function mapred_subcommand_ecvalidator
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.ec.ECValidatorJob
  hadoop_add_to_classpath_tools hadoop-shopee
}

fi

if ! declare -f mapred_subcommand_compatibilitytest >/dev/null 2>/dev/null; then

  if [[ "${HADOOP_SHELL_EXECNAME}" = mapred ]]; then
    hadoop_add_subcommand "compatibilitytest" client "run a compatibility test between different hadoop versions"
  fi

  # this can't be indented otherwise shelldocs won't get it

## @description  compatibilitytest command
## @audience     public
## @stability    stable
## @replaceable  yes
function mapred_subcommand_compatibilitytest
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.compatibility.TestCase
  hadoop_add_to_classpath_tools hadoop-shopee
}

fi


if ! declare -f hadoop_subcommand_nsmigrate >/dev/null 2>/dev/null; then
  if [[ "${HADOOP_SHELL_EXECNAME}" = hadoop ]]; then
    hadoop_add_subcommand "nsmigrate" client "migrate a directory across namespaces"
  fi

## @description  nsmigrate
## @audience     public
## @stability    stable
## @replaceable  yes
function hadoop_subcommand_nsmigrate
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.federation.migration.NSMigrationTool
  hadoop_add_to_classpath_tools hadoop-shopee
}

fi


if ! declare -f mapred_subcommand_nsmigrate >/dev/null 2>/dev/null; then

  if [[ "${HADOOP_SHELL_EXECNAME}" = mapred ]]; then
    hadoop_add_subcommand "nsmigrate" client "migrate a directory across namespaces"
  fi

  # this can't be indented otherwise shelldocs won't get it

## @description  nsmigrate
## @audience     public
## @stability    stable
## @replaceable  yes
function mapred_subcommand_nsmigrate
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.federation.migration.NSMigrationTool
  hadoop_add_to_classpath_tools hadoop-shopee
}

fi


if ! declare -f hadoop_subcommand_datacleanup >/dev/null 2>/dev/null; then
  if [[ "${HADOOP_SHELL_EXECNAME}" = hadoop ]]; then
    hadoop_add_subcommand "datacleanup" client "clean up old data"
  fi

## @description  datacleanup
## @audience     public
## @stability    stable
## @replaceable  yes
function hadoop_subcommand_datacleanup
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.DataCleanup
  hadoop_add_to_classpath_tools hadoop-shopee
}

fi


if ! declare -f mapred_subcommand_datacleanup >/dev/null 2>/dev/null; then

  if [[ "${HADOOP_SHELL_EXECNAME}" = mapred ]]; then
    hadoop_add_subcommand "datacleanup" client "clean up old data"
  fi

  # this can't be indented otherwise shelldocs won't get it

## @description  datacleanup
## @audience     public
## @stability    stable
## @replaceable  yes
function mapred_subcommand_datacleanup
{
  # shellcheck disable=SC2034
  HADOOP_CLASSNAME=org.apache.hadoop.tools.DataCleanup
  hadoop_add_to_classpath_tools hadoop-shopee
}

fi
