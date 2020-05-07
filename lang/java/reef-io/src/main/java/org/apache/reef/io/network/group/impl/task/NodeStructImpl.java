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

import org.apache.reef.io.network.group.api.task.NodeStruct;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.utils.Utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public abstract class NodeStructImpl implements NodeStruct {

  private static final Logger LOG = Logger.getLogger(NodeStructImpl.class.getName());

  private final String id;
  private final BlockingQueue<GroupCommunicationMessage> dataQue = new LinkedBlockingQueue<>();

  private int version;

  public NodeStructImpl(final String id, final int version) {
    super();
    this.id = id;
    this.version = version;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public void setVersion(final int version) {
    this.version = version;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void addData(final GroupCommunicationMessage msg) {
    LOG.entering("NodeStructImpl", "addData", msg);
    dataQue.add(msg);
    LOG.exiting("NodeStructImpl", "addData", msg);
  }

  @Override
  public byte[] getData() {
    LOG.entering("NodeStructImpl", "getData");
    final GroupCommunicationMessage gcm;
    try {
      gcm = dataQue.take();
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for data from " + id, e);
    }

    final byte[] retVal = checkDead(gcm) ? null : Utils.getData(gcm);
    LOG.exiting("NodeStructImpl", "getData", retVal);
    return retVal;
  }

  @Override
  public String toString() {
    return "(" + id + "," + version + ")";
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof NodeStructImpl) {
      final NodeStructImpl that = (NodeStructImpl) obj;
      return this.id.equals(that.id) && this.version == that.version;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * id.hashCode() + version;
  }

  public abstract boolean checkDead(GroupCommunicationMessage gcm);
}
