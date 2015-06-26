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
 * Default POJO implementation of ResourceAllocationEvent.
 * Use newBuilder to construct an instance.
 */
public final class ResourceAllocationEventImpl implements ResourceAllocationEvent {
  private final String identifier;
  private final int resourceMemory;
  private final String nodeId;
  private final Optional<Integer> virtualCores;
  private final Optional<String> rackName;


  private ResourceAllocationEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.resourceMemory = BuilderUtils.notNull(builder.resourceMemory);
    this.nodeId = BuilderUtils.notNull(builder.nodeId);
    this.virtualCores = Optional.ofNullable(builder.virtualCores);
    this.rackName = Optional.ofNullable(builder.rackName);
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public int getResourceMemory() {
    return resourceMemory;
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  @Override
  public Optional<Integer> getVirtualCores() {
    return virtualCores;
  }

  @Override
  public Optional<String> getRackName() {
    return rackName;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create ResourceAllocationEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<ResourceAllocationEvent> {
    private String identifier;
    private Integer resourceMemory;
    private String nodeId;
    private Integer virtualCores;
    private String rackName;


    /**
     * @see ResourceAllocationEvent#getIdentifier()
     */
    public Builder setIdentifier(final String identifier) {
      this.identifier = identifier;
      return this;
    }

    /**
     * @see ResourceAllocationEvent#getResourceMemory()
     */
    public Builder setResourceMemory(final int resourceMemory) {
      this.resourceMemory = resourceMemory;
      return this;
    }

    /**
     * @see ResourceAllocationEvent#getNodeId()
     */
    public Builder setNodeId(final String nodeId) {
      this.nodeId = nodeId;
      return this;
    }

    /**
     * @see ResourceAllocationEvent#getVirtualCores()
     */
    public Builder setVirtualCores(final int virtualCores) {
      this.virtualCores = virtualCores;
      return this;
    }

    /**
     * @see ResourceAllocationEvent#getRackName()
     */
    public Builder setRackName(final String rackName) {
      this.rackName = rackName;
      return this;
    }

    @Override
    public ResourceAllocationEvent build() {
      return new ResourceAllocationEventImpl(this);
    }
  }
}
