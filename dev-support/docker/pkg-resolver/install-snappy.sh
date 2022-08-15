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
mkdir -p /opt/snappy-src &&
  curl -L -s -S \
    https://github.com/google/snappy/archive/refs/tags/1.1.3.tar.gz \
    -o /opt/snappy.tar.gz &&
  tar xzf /opt/snappy.tar.gz --strip-components 1 -C /opt/snappy-src&&
  cd /opt/snappy-src &&
  sh autogen.sh &&
  sh configure --prefix=/usr/lib/snappy &&
  make "-j$(nproc)" &&
  make install &&
  cd /root &&
  rm -rf /opt/snappy-src

