/*
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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.tang.annotations.Name;

/**
 * Helper class to wrap msg and the operator name in the msg.
 */
public class IndexedMsg {
  private final Class<? extends Name<String>> operName;
  private final GroupCommunicationMessage msg;

  public IndexedMsg(final GroupCommunicationMessage msg) {
    super();
    this.operName = Utils.getClass(msg.getOperatorname());
    this.msg = msg;
  }

  public Class<? extends Name<String>> getOperName() {
    return operName;
  }

  public GroupCommunicationMessage getMsg() {
    return msg;
  }

  @Override
  public int hashCode() {
    return operName.getName().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof IndexedMsg)) {
      return false;
    }
    final IndexedMsg that = (IndexedMsg) obj;
    if (this.operName == that.operName) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return operName.getSimpleName();
  }

}
