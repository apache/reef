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
package org.apache.reef.runtime.common.driver.catalog;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.RackDescriptor;

import java.net.InetSocketAddress;

@Private
public class NodeDescriptorImpl implements NodeDescriptor {

  private final RackDescriptorImpl rack;

  private final String id;

  private final InetSocketAddress address;

  private final int ram;

  /**
   * @param id
   * @param address
   * @param rack
   * @param ram     the RAM available to the machine, in MegaBytes.
   */
  NodeDescriptorImpl(final String id, final InetSocketAddress address, final RackDescriptorImpl rack, final int ram) {
    this.id = id;
    this.address = address;
    this.rack = rack;
    this.ram = ram;
    this.rack.addNodeDescriptor(this);
  }

  @Override
  public String toString() {
    return "Node [" + this.address + "]: RACK " + this.rack.getName() + ", RAM " + ram;
  }

  @Override
  public final String getId() {
    return this.id;
  }


  @Override
  public InetSocketAddress getInetSocketAddress() {
    return this.address;
  }

  @Override
  public RackDescriptor getRackDescriptor() {
    return this.rack;
  }

  @Override
  public String getName() {
    return this.address.getHostName();
  }

}
