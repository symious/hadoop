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
mkdir -p /opt/lz4-src &&
  curl -L -s -S \
    https://github.com/lz4/lz4/archive/refs/tags/v1.9.0.tar.gz \
    -o /opt/lz4.tar.gz &&
  tar xzf /opt/lz4.tar.gz --strip-components 1 -C /opt/lz4-src &&
  cd /opt/lz4-src &&
  make &&
  make install &&
  cd /root &&
  rm -rf /opt/lz4-src
