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

import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.util.BuilderUtils;
import org.apache.reef.util.Optional;

import java.util.ArrayList;
import java.util.List;

/**
 * Default POJO implementation of RuntimeStatusEvent.
 * Use newBuilder to construct an instance.
 */
public final class RuntimeStatusEventImpl implements RuntimeStatusEvent {

  private final String name;
  private final State state;
  private final List<String> containerAllocationList;
  private final Optional<ReefServiceProtos.RuntimeErrorProto> error;
  private final Optional<Integer> outstandingContainerRequests;

  private RuntimeStatusEventImpl(final Builder builder) {
    this.name = BuilderUtils.notNull(builder.name);
    this.state = BuilderUtils.notNull(builder.state);
    this.containerAllocationList = BuilderUtils.notNull(builder.containerAllocationList);
    this.error = Optional.ofNullable(builder.error);
    this.outstandingContainerRequests = Optional.ofNullable(builder.outstandingContainerRequests);
  }

  @Override
  public String toString() {

    // Replace with String.join() after migration to Java 1.8
    final StringBuilder allocatedContainers = new StringBuilder();
    for (String container : this.containerAllocationList) {
      if (allocatedContainers.length() > 0) {
        allocatedContainers.append(',');
      }
      allocatedContainers.append(container);
    }

    return String.format(
        "RuntimeStatusEventImpl:{name:%s, state:%s, allocated:[%s], outstanding:%d, error:%s}",
        this.name, this.state, allocatedContainers, this.outstandingContainerRequests.orElse(0),
        this.error.isPresent());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public List<String> getContainerAllocationList() {
    return containerAllocationList;
  }

  @Override
  public Optional<ReefServiceProtos.RuntimeErrorProto> getError() {
    return error;
  }

  @Override
  public Optional<Integer> getOutstandingContainerRequests() {
    return outstandingContainerRequests;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create RuntimeStatusEvent instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<RuntimeStatusEvent> {
    private String name;
    private State state;
    private List<String> containerAllocationList = new ArrayList<>();
    private ReefServiceProtos.RuntimeErrorProto error;
    private Integer outstandingContainerRequests;

    /**
     * @see RuntimeStatusEvent#getName()
     */
    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    /**
     * @see RuntimeStatusEvent#getState()
     */
    public Builder setState(final State state) {
      this.state = state;
      return this;
    }

    /**
     * Add an entry to containerAllocationList.
     * @see RuntimeStatusEvent#getContainerAllocationList()
     */
    public Builder addContainerAllocation(final String containerAllocation) {
      this.containerAllocationList.add(containerAllocation);
      return this;
    }

    /**
     * @see RuntimeStatusEvent#getError()
     */
    public Builder setError(final ReefServiceProtos.RuntimeErrorProto error) {
      this.error = error;
      return this;
    }

    /**
     * @see RuntimeStatusEvent#getOutstandingContainerRequests()
     */
    public Builder setOutstandingContainerRequests(final int outstandingContainerRequests) {
      this.outstandingContainerRequests = outstandingContainerRequests;
      return this;
    }

    @Override
    public RuntimeStatusEvent build() {
      return new RuntimeStatusEventImpl(this);
    }
  }
}
