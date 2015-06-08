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
package org.apache.reef.io.network.group.impl.task;

import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;

import java.util.logging.Logger;

public class ChildNodeStruct extends NodeStructImpl {

  private static final Logger LOG = Logger.getLogger(ChildNodeStruct.class.getName());

  public ChildNodeStruct(final String id, final int version) {
    super(id, version);
  }

  @Override
  public boolean checkDead(final GroupCommunicationMessage gcm) {
    LOG.entering("ChildNodeStruct", "checkDead", gcm);
    final boolean retVal = gcm.getType() == ReefNetworkGroupCommProtos.GroupCommMessage.Type.ChildDead ? true : false;
    LOG.exiting("ChildNodeStruct", "checkDead", gcm);
    return retVal;
  }

}
