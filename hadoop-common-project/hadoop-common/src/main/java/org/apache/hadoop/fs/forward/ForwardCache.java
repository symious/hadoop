/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.forward;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_FORWARD_RULES;

public class ForwardCache {
  public static final Logger LOG = LoggerFactory.getLogger(ForwardCache.class);

  private static final String RULE_SEPERATOR = ",";

  private static AtomicBoolean initialized = new AtomicBoolean(false);

  private static Map<SchemeAndAuthority, SchemeAndAuthority> forwardMap =
      new HashMap<>();

  private static synchronized void initialize(Configuration conf) {
    if (initialized.get()) {
      return;
    }
    String forwardRulesValue = conf.get(FS_FORWARD_RULES, "");
    if (forwardRulesValue.isEmpty()) {
      return;
    }
    if (forwardRulesValue.split(RULE_SEPERATOR).length % 2 != 0) {
      LOG.error(FS_FORWARD_RULES + " content length error.");
      return;
    }
    String[] forwardRules = forwardRulesValue.split(RULE_SEPERATOR);
    for (int i = 0; i < forwardRules.length; i++) {
      String src = forwardRules[i];
      String dest = forwardRules[++i];
      try {
        SchemeAndAuthority srcSA = new SchemeAndAuthority(new URI(src));
        SchemeAndAuthority destSA = new SchemeAndAuthority(new URI(dest));
        if (srcSA.isAvailable() && destSA.isAvailable()) {
          forwardMap.put(srcSA, destSA);
        } else {
          LOG.warn("Can not be translated to forward rules: " + src +
              " and " + dest + ".");
        }
      } catch (URISyntaxException e) {
        LOG.error("Error processing " + src + " and " + dest + ".", e);
      }
    }
    initialized.compareAndSet(false, true);
  }

  public static URI forward(URI uri, Configuration conf) {
    initialize(conf);
    URI res = uri;
    SchemeAndAuthority oriSA = new SchemeAndAuthority(uri);
    if (forwardMap.containsKey(oriSA)) {
      SchemeAndAuthority value = forwardMap.get(oriSA);
      try {
        res = new URI(value.getScheme(), value.getAuthority(), uri.getPath(),
            uri.getQuery(), uri.getFragment());
      } catch (URISyntaxException e) {
        LOG.error("Error when forwarding URI: origin uri: " + uri +
            ", dest scheme: " + value.getScheme() +
            ", dest authority: " + value.getAuthority(), e);
      }
    }
    return res;
  }

  public static SchemeAndAuthority forward(SchemeAndAuthority srcSA,
      Configuration conf) {
    initialize(conf);
    if (forwardMap.containsKey(srcSA)) {
      return forwardMap.get(srcSA);
    }
    return srcSA;
  }

  public static boolean isBeingForward(URI uri, Configuration conf) {
    initialize(conf);
    SchemeAndAuthority checkSA = new SchemeAndAuthority(uri);
    return forwardMap.containsKey(checkSA);
  }
}