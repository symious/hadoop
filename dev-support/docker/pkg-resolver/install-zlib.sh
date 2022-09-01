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
mkdir -p /opt/zlib-src &&
  curl -L -s -S \
    https://github.com/madler/zlib/archive/refs/tags/v1.2.7.tar.gz \
    -o /opt/zlib.tar.gz &&
  tar xzf /opt/zlib.tar.gz --strip-components 1 -C /opt/zlib-src &&
  cd /opt/zlib-src &&
  sh ./configure --prefix=/usr/lib/zlib &&
  make &&
  make install &&
  cd /root &&
  rm -rf /opt/zlib-src

