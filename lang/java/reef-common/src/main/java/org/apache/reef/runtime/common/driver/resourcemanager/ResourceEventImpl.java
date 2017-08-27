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

import java.util.Map;

/**
 * Default POJO implementation of ResourceAllocationEvent and ResourceRecoverEvent.
 * Use newAllocationBuilder to construct an instance for ResourceAllocationEvent and
 * use newRecoveryBuilder to construct an instance for ResourceRecoverEvent.
 */
public final class ResourceEventImpl implements ResourceAllocationEvent, ResourceRecoverEvent {
  private final String identifier;
  private final int resourceMemory;
  private final String nodeId;
  private final Optional<Integer> virtualCores;
  private final Optional<String> rackName;
  private final String runtimeName;
  private final Map<String, String> nodeLabels;


  private ResourceEventImpl(final Builder builder) {
    this.identifier = BuilderUtils.notNull(builder.identifier);
    this.resourceMemory = builder.recovery ? builder.resourceMemory : BuilderUtils.notNull(builder.resourceMemory);
    this.nodeId = builder.recovery ? builder.nodeId : BuilderUtils.notNull(builder.nodeId);
    this.virtualCores = Optional.ofNullable(builder.virtualCores);
    this.rackName = Optional.ofNullable(builder.rackName);
    this.runtimeName = BuilderUtils.notNull(builder.runtimeName);
    this.nodeLabels = builder.nodeLabels;
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

  @Override
  public String getRuntimeName() {
    return runtimeName;
  }

  @Override
  public Map<String, String> getNodeLabels() {
    return nodeLabels;
  }

  public static Builder newAllocationBuilder() {
    return new Builder(false);
  }

  public static Builder newRecoveryBuilder() {
    return new Builder(true);
  }

  /**
   * Builder used to create ResourceAllocationEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<ResourceEventImpl> {
    private final boolean recovery;

    private String identifier;
    private Integer resourceMemory;
    private String nodeId;
    private Integer virtualCores;
    private String rackName;
    private String runtimeName;
    private Map<String, String> nodeLabels;

    private Builder(final boolean recovery){
      this.recovery = recovery;
    }

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

    /**
     * @see ResourceAllocationEvent#getRuntimeName()
     */
    public Builder setRuntimeName(final String runtimeName) {
      this.runtimeName = runtimeName;
      return this;
    }

    /**
     * @see ResourceAllocationEvent#getNodeLabels()
     * @param nodeLabels
     */
    public Builder setNodeLabels(final Map<String, String> nodeLabels) {
      this.nodeLabels = nodeLabels;
      return this;
    }


    @Override
    public ResourceEventImpl build() {
      return new ResourceEventImpl(this);
    }
  }
}
