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
package com.microsoft.reef.runtime.common.driver.catalog;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.RackDescriptor;
import com.microsoft.reef.driver.catalog.ResourceCatalog;
import com.microsoft.reef.proto.DriverRuntimeProtocol.NodeDescriptorProto;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Private
public final class ResourceCatalogImpl implements ResourceCatalog {

  private static final Logger LOG = Logger.getLogger(ResourceCatalog.class.getName());

  public final static String DEFAULT_RACK = "/default-rack";

  private final Map<String, RackDescriptorImpl> racks = new HashMap<>();

  private final Map<String, NodeDescriptorImpl> nodes = new HashMap<>();

  @Inject
  ResourceCatalogImpl() {
    LOG.log(Level.FINE, "Instantiated 'ResourceCatalogImpl'");
  }

  @Override
  public synchronized String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("=== Resource Catalog ===");
    for (final RackDescriptor rack : racks.values()) {
      sb.append("\n" + rack);
    }
    return sb.toString();
  }

  @Override
  public synchronized Collection<NodeDescriptor> getNodes() {
    return Collections.unmodifiableCollection(new ArrayList<NodeDescriptor>(this.nodes.values()));
  }

  @Override
  public synchronized Collection<RackDescriptor> getRacks() {
    return Collections.unmodifiableCollection(new ArrayList<RackDescriptor>(this.racks.values()));
  }

  public synchronized final NodeDescriptor getNode(final String id) {
    return this.nodes.get(id);
  }

  public synchronized final void handle(final NodeDescriptorProto node) {
    final String rack_name = (node.hasRackName() ? node.getRackName() : DEFAULT_RACK);

    LOG.log(Level.FINEST, "Catalog new node: id[{0}], rack[{1}], host[{2}], port[{3}], memory[{4}]",
        new Object[]{node.getIdentifier(), rack_name, node.getHostName(), node.getPort(),
            node.getMemorySize()}
    );

    if (!this.racks.containsKey(rack_name)) {
      final RackDescriptorImpl rack = new RackDescriptorImpl(rack_name);
      this.racks.put(rack_name, rack);
    }
    final RackDescriptorImpl rack = this.racks.get(rack_name);
    final InetSocketAddress address = new InetSocketAddress(node.getHostName(), node.getPort());
    final NodeDescriptorImpl nodeDescriptor = new NodeDescriptorImpl(node.getIdentifier(), address, rack, node.getMemorySize());
    this.nodes.put(nodeDescriptor.getId(), nodeDescriptor);
  }
}
