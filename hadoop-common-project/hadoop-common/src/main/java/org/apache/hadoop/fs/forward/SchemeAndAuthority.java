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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class SchemeAndAuthority {
  private String scheme = null ;
  private String authority = null;

  SchemeAndAuthority(String scheme, String authority) {
    this.scheme = scheme;
    this.authority = authority;
  }

  public SchemeAndAuthority(URI uri) {
    if (uri.getScheme() != null && !uri.getScheme().isEmpty()) {
      this.scheme = uri.getScheme();
    } else {
      this.scheme = null;
    }
    if (uri.getAuthority() != null && !uri.getAuthority().isEmpty()) {
      this.authority = uri.getAuthority();
    } else {
      this.authority = null;
    }
  }

  public boolean isAvailable() {
    return scheme != null && authority != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemeAndAuthority that = (SchemeAndAuthority) o;
    return Objects.equals(scheme, that.scheme) &&
        Objects.equals(authority, that.authority);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheme, authority);
  }

  public URI toUri() throws URISyntaxException {
    return new URI(scheme, authority, "/", null, null);
  }

  public String getScheme() {
    return scheme;
  }

  public void setScheme(String scheme) {
    this.scheme = scheme;
  }

  public String getAuthority() {
    return authority;
  }

  public void setAuthority(String authority) {
    this.authority = authority;
  }
}
