/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.annotations.Name;

/**
 *
 */
public class IndexedMsg {
  private final Class<? extends Name<String>> operName;
  private final GroupCommMessage msg;

  public IndexedMsg(final GroupCommMessage msg) {
    super();
    this.operName = Utils.getClass(msg.getOperatorname());
    this.msg = msg;
  }


  public Class<? extends Name<String>> getOperName() {
    return operName;
  }


  public GroupCommMessage getMsg() {
    return msg;
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

}
