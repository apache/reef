/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.test.remote;

import org.apache.reef.wake.remote.RemoteIdentifier;

public class TestRemoteIdentifier implements RemoteIdentifier {
  private final String str;

  public TestRemoteIdentifier(String str) {
    this.str = str;
  }

  public String getString() {
    return str;
  }

  public boolean equals(Object o) {
    return str.equals(((TestRemoteIdentifier) o).getString());
  }

  public int hashCode() {
    return str.hashCode();
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("test://");
    builder.append(str);
    return builder.toString();
  }
}
