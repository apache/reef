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
package org.apache.reef.runtime.common.driver.api;

import org.apache.reef.util.BuilderUtils;
import org.apache.reef.util.Optional;

import java.util.ArrayList;
import java.util.List;

/**
 * Default POJO implementation of ResourceRequestEvent.
 * Use newBuilder to construct an instance.
 */
public final class ResourceRequestEventImpl implements ResourceRequestEvent {
  private final int resourceCount;
  private final List<String> nodeNameList;
  private final List<String> rackNameList;
  private final Optional<Integer> memorySize;
  private final Optional<Integer> priority;
  private final Optional<Integer> virtualCores;
  private final Optional<Boolean> relaxLocality;
  private final String runtimeName;

  private ResourceRequestEventImpl(final Builder builder) {
    this.resourceCount = BuilderUtils.notNull(builder.resourceCount);
    this.nodeNameList = BuilderUtils.notNull(builder.nodeNameList);
    this.rackNameList = BuilderUtils.notNull(builder.rackNameList);
    this.memorySize = Optional.ofNullable(builder.memorySize);
    this.priority = Optional.ofNullable(builder.priority);
    this.virtualCores = Optional.ofNullable(builder.virtualCores);
    this.relaxLocality = Optional.ofNullable(builder.relaxLocality);
    this.runtimeName = BuilderUtils.notNull(builder.runtimeName);
  }

  @Override
  public int getResourceCount() {
    return resourceCount;
  }

  @Override
  public List<String> getNodeNameList() {
    return nodeNameList;
  }

  @Override
  public List<String> getRackNameList() {
    return rackNameList;
  }

  @Override
  public Optional<Integer> getMemorySize() {
    return memorySize;
  }

  @Override
  public Optional<Integer> getPriority() {
    return priority;
  }

  @Override
  public Optional<Integer> getVirtualCores() {
    return virtualCores;
  }

  @Override
  public Optional<Boolean> getRelaxLocality() {
    return relaxLocality;
  }

  @Override
  public String getRuntimeName() {
    return runtimeName;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create ResourceRequestEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<ResourceRequestEvent> {
    private Integer resourceCount;
    private List<String> nodeNameList = new ArrayList<>();
    private List<String> rackNameList = new ArrayList<>();
    private Integer memorySize;
    private Integer priority;
    private Integer virtualCores;
    private Boolean relaxLocality;
    private String runtimeName;

    /**
     * Create a builder from an existing ResourceRequestEvent.
     */
    public Builder mergeFrom(final ResourceRequestEvent resourceRequestEvent) {
      this.resourceCount = resourceRequestEvent.getResourceCount();
      this.nodeNameList = resourceRequestEvent.getNodeNameList();
      this.rackNameList = resourceRequestEvent.getRackNameList();
      this.memorySize = resourceRequestEvent.getMemorySize().orElse(null);
      this.priority = resourceRequestEvent.getPriority().orElse(null);
      this.virtualCores = resourceRequestEvent.getVirtualCores().orElse(null);
      this.relaxLocality = resourceRequestEvent.getRelaxLocality().orElse(null);
      this.runtimeName = resourceRequestEvent.getRuntimeName();
      return this;
    }

    /**
     * @see ResourceRequestEvent#getResourceCount()
     */
    public Builder setResourceCount(final int resourceCount) {
      this.resourceCount = resourceCount;
      return this;
    }

    /**
     * Add an entry to the nodeNameList.
     * @see ResourceRequestEvent#getNodeNameList()
     */
    public Builder addNodeName(final String nodeName) {
      this.nodeNameList.add(nodeName);
      return this;
    }

    /**
     * Add a list of node names.
     * @see ResourceRequestEventImpl.Builder#addNodeName
     */
    public Builder addNodeNames(final List<String> nodeNames) {
      for (final String nodeName : nodeNames) {
        addNodeName(nodeName);
      }
      return this;
    }

    /**
     * Add a list of rack names.
     * @see ResourceRequestEventImpl.Builder#addRackName
     */
    public Builder addRackName(final String rackName) {
      this.rackNameList.add(rackName);
      return this;
    }

    /**
     * Add an entry to rackNameList.
     * @see ResourceRequestEvent#getRackNameList
     */
    public Builder addRackNames(final List<String> rackNames) {
      for (final String rackName : rackNames) {
        addRackName(rackName);
      }
      return this;
    }

    /**
     * @see ResourceRequestEvent#getMemorySize
     */
    public Builder setMemorySize(final int memorySize) {
      this.memorySize = memorySize;
      return this;
    }

    /**
     * @see ResourceRequestEvent#getPriority
     */
    public Builder setPriority(final int priority) {
      this.priority = priority;
      return this;
    }

    /**
     * @see ResourceRequestEvent#getVirtualCores
     */
    public Builder setVirtualCores(final int virtualCores) {
      this.virtualCores = virtualCores;
      return this;
    }

    /**
     * @see ResourceRequestEvent#getRelaxLocality
     */
    public Builder setRelaxLocality(final boolean relaxLocality) {
      this.relaxLocality = relaxLocality;
      return this;
    }

    /**
     * @see ResourceRequestEvent#getRelaxLocality
     */
    public Builder setRuntimeName(final String runtimeName) {
      this.runtimeName = runtimeName;
      return this;
    }

    @Override
    public ResourceRequestEvent build() {
      return new ResourceRequestEventImpl(this);
    }
  }
}
