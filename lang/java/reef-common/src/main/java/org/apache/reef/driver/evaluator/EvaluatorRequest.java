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
package org.apache.reef.driver.evaluator;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A request for one ore more Evaluators.
 */
@Public
@DriverSide
@Provided
public final class EvaluatorRequest {

  private final int megaBytes;
  private final int number;
  private final int cores;
  private final List<String> nodeNames;
  private final List<String> rackNames;
  private final String runtimeName;

  EvaluatorRequest(final int number,
                   final int megaBytes,
                   final int cores,
                   final List<String> nodeNames,
                   final List<String> rackNames) {
    this(number, megaBytes, cores, nodeNames, rackNames, "");
  }

  EvaluatorRequest(final int number,
                   final int megaBytes,
                   final int cores,
                   final List<String> nodeNames,
                   final List<String> rackNames,
                   final String runtimeName) {
    this.number = number;
    this.megaBytes = megaBytes;
    this.cores = cores;
    this.nodeNames = nodeNames;
    this.rackNames = rackNames;
    this.runtimeName = runtimeName;
  }

  /**
   * Get a new builder.
   *
   * @return a new EvaluatorRequest Builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Get a new builder from the existing request.
   *
   * @return a new EvaluatorRequest Builder with settings initialized
   * from an existing request.
   */
  public static Builder newBuilder(final EvaluatorRequest request) {
    return new Builder(request);
  }

  /**
   * Access the number of Evaluators requested.
   *
   * @return the number of Evaluators requested.
   */
  public int getNumber() {
    return this.number;
  }

  /**
   * Access the number of core of Evaluators requested.
   *
   * @return the number of cores requested.
   */
  public int getNumberOfCores() {
    return this.cores;
  }

  /**
   * Access the number of memory requested.
   *
   * @return the minimum size of Evaluator requested.
   */
  public int getMegaBytes() {
    return megaBytes;
  }

  /**
   * Access the preferred node.
   *
   * @return the node names that we prefer the Evaluator to run on
   */
  public List<String> getNodeNames() {
    return Collections.unmodifiableList(nodeNames);
  }

  /**
   * Access the preferred rack name.
   *
   * @return the rack names that we prefer the Evaluator to run on
   */
  public List<String> getRackNames() {
    return Collections.unmodifiableList(rackNames);
  }

  /**
   * Access the required runtime name.
   *
   * @return the runtime name that we need the Evaluator to run on
   */
  public String getRuntimeName() {
    return runtimeName;
  }

  /**
   * {@link EvaluatorRequest}s are build using this Builder.
   */
  public static final class Builder implements org.apache.reef.util.Builder<EvaluatorRequest> {

    private int n = 1;
    private int megaBytes = -1;
    private int cores = 1; //if not set, default to 1
    private final List<String> nodeNames = new ArrayList<>();
    private final List<String> rackNames = new ArrayList<>();
    private String runtimeName = "";

    private Builder() {
    }

    /**
     * Pre-populates the builder with the values extracted from the request.
     *
     * @param request the request
     * @return this Builder
     */
    private Builder(final EvaluatorRequest request) {
      setNumber(request.getNumber());
      setMemory(request.getMegaBytes());
      setNumberOfCores(request.getNumberOfCores());
      setRuntimeName(request.getRuntimeName());
      for (final String nodeName : request.getNodeNames()) {
        addNodeName(nodeName);
      }
      for (final String rackName : request.getRackNames()) {
        addRackName(rackName);
      }
    }

    /**
     * Set the amount of memory.
     *
     * @param megaBytes the amount of megabytes to request for the Evaluator.
     * @return this builder
     */
    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setMemory(final int megaBytes) {
      this.megaBytes = megaBytes;
      return this;
    }

    /**
     * Set the name of the desired runtime.
     *
     * @param runtimeName to request for the Evaluator.
     * @return this builder
     */
    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setRuntimeName(final String runtimeName) {
      this.runtimeName = runtimeName;
      return this;
    }

    /**
     * Set number of cores.
     *
     * @param cores the number of cores
     * @return this Builder.
     */
    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setNumberOfCores(final int cores) {
      this.cores = cores;
      return this;
    }

    /**
     * Set the number of Evaluators requested.
     *
     * @param n the number of evaluators
     * @return this Builder.
     */
    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setNumber(final int n) {
      this.n = n;
      return this;
    }

    /**
     * Adds a node name.It is the preferred location where the evaluator should
     * run on. If the node is available, the RM will try to allocate the
     * evaluator there
     *
     * @param nodeName a preferred node name
     * @return this Builder.
     */
    public Builder addNodeName(final String nodeName) {
      this.nodeNames.add(nodeName);
      return this;
    }

    /**
     * Adds a rack name. It is the preferred location where the evaluator should
     * run on. If the rack is available, the RM will try to allocate the
     * evaluator in one of its nodes. The RM will try to match node names first,
     * and then fallback to rack names
     *
     * @param rackName a preferred rack name
     * @return this Builder.
     */
    public Builder addRackName(final String rackName) {
      this.rackNames.add(rackName);
      return this;
    }

    /**
     * Builds the {@link EvaluatorRequest}.
     */
    @Override
    public EvaluatorRequest build() {
      return new EvaluatorRequest(this.n, this.megaBytes, this.cores, this.nodeNames, this.rackNames, this.runtimeName);
    }
  }
}
