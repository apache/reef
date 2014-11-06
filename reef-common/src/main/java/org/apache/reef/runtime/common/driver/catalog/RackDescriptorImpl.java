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

import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.RackDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class RackDescriptorImpl implements RackDescriptor {

  private final String name;


  private final List<NodeDescriptorImpl> nodes;

  RackDescriptorImpl(final String name) {
    this.name = name;
    this.nodes = new ArrayList<>();
  }

  public final String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Rack " + this.name);
    for (final NodeDescriptorImpl node : nodes) {
      sb.append("\n\t" + node);
    }
    return sb.toString();
  }

  public final int hashCode() {
    return this.name.hashCode();
  }

  public final boolean equals(final Object obj) {
    if (obj instanceof RackDescriptorImpl) {
      return obj.toString().equals(this.name);
    } else {
      return false;
    }
  }

  public String getName() {
    return this.name;
  }

  @Override
  public List<NodeDescriptor> getNodes() {
    return Collections.unmodifiableList(new ArrayList<NodeDescriptor>(this.nodes));
  }

  /**
   * Should only be used from YarnNodeDescriptor constructor.
   *
   * @param node to add.
   */
  void addNodeDescriptor(final NodeDescriptorImpl node) {
    this.nodes.add(node);
  }
}
