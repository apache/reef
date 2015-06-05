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
package org.apache.reef.runtime.common.driver.resourcemanager;

import org.apache.reef.util.BuilderUtils;
import org.apache.reef.util.Optional;

/**
 * Default POJO implementation of NodeDescriptorEvent.
 * Use newBuilder to construct an instance.
 */
public final class NodeDescriptorEventImpl implements NodeDescriptorEvent {
  private final String identifier;
  private final String hostName;
  private final int port;
  private final int memorySize;
  private final Optional<String> rackName;

  private NodeDescriptorEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.hostName = BuilderUtils.notNull(builder.hostName);
    this.port = BuilderUtils.notNull(builder.port);
    this.memorySize = BuilderUtils.notNull(builder.memorySize);
    this.rackName = Optional.ofNullable(builder.rackName);
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public String getHostName() {
    return hostName;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public int getMemorySize() {
    return memorySize;
  }

  @Override
  public Optional<String> getRackName() {
    return rackName;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create NodeDescriptorEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<NodeDescriptorEvent> {
    private String identifier;
    private String hostName;
    private Integer port;
    private Integer memorySize;
    private String rackName;

    /**
     * @see NodeDescriptorEvent#getIdentifier()
     */
    public Builder setIdentifier(final String identifier) {
      this.identifier = identifier;
      return this;
    }

    /**
     * @see NodeDescriptorEvent#getHostName()
     */
    public Builder setHostName(final String hostName) {
      this.hostName = hostName;
      return this;
    }

    /**
     * @see NodeDescriptorEvent#getPort()
     */
    public Builder setPort(final int port) {
      this.port = port;
      return this;
    }

    /**
     * @see NodeDescriptorEvent#getMemorySize()
     */
    public Builder setMemorySize(final int memorySize) {
      this.memorySize = memorySize;
      return this;
    }

    /**
     * @see NodeDescriptorEvent#getRackName()
     */
    public Builder setRackName(final String rackName) {
      this.rackName = rackName;
      return this;
    }

    @Override
    public NodeDescriptorEvent build() {
      return new NodeDescriptorEventImpl(this);
    }
  }
}
