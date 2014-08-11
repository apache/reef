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

import com.microsoft.reef.io.network.nggroup.api.NodeStruct;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 *
 */
public abstract class NodeStructImpl implements NodeStruct {

  private static final Logger LOG = Logger.getLogger(NodeStructImpl.class.getName());


  private final String id;
  private final BlockingQueue<GroupCommMessage> dataQue = new LinkedBlockingQueue<>();


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
  public void addData(final GroupCommMessage msg) {
    dataQue.add(msg);
  }

  @Override
  public byte[] getData() {
    GroupCommMessage gcm;
    try {
      gcm = dataQue.take();
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while waiting for data from " + id, e);
    }

    if (checkDead(gcm)) {
      return null;
    }

    return Utils.getData(gcm);
  }

  @Override
  public String toString() {
    return id + ":ver(" + version + ")";
  }

  @Override
  public boolean equals(final Object obj) {
    if(!(obj instanceof NodeStructImpl)) {
      return false;
    }
    final NodeStructImpl that = (NodeStructImpl) obj;
    return this.id.equals(that.id) && this.version==that.version;
  }

  public abstract boolean checkDead(final GroupCommMessage gcm);

}
