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
import org.apache.reef.driver.catalog.ResourceCatalog;
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
  @Deprecated
  private final ResourceCatalog.Descriptor descriptor;
  private final List<String> nodeNames;
  private final List<String> rackNames;

  @Deprecated
  EvaluatorRequest(final int number,
                   final int megaBytes,
                   final int cores,
                   final ResourceCatalog.Descriptor descriptor) {
    this(number, megaBytes, cores, new ArrayList<String>(), new ArrayList<String>());
  }

  EvaluatorRequest(final int number,
      final int megaBytes,
      final int cores,
      final List<String> nodeNames,
      final List<String> rackNames) {
    this.number = number;
    this.megaBytes = megaBytes;
    this.cores = cores;
    this.nodeNames = nodeNames;
    this.rackNames = rackNames;
    this.descriptor = null;
  }

  /**
   * @return a new EvaluatorRequest Builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
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
   * Access the {@link org.apache.reef.driver.catalog.NodeDescriptor} used as the template for this
   * {@link EvaluatorRequest}.
   *
   * @return the {@link org.apache.reef.driver.catalog.NodeDescriptor} used as the template for this
   * {@link EvaluatorRequest}.
   */
  @Deprecated
  public ResourceCatalog.Descriptor getDescriptor() {
    return this.descriptor;
  }

  /**
   * @return the minimum size of Evaluator requested.
   */
  public int getMegaBytes() {
    return megaBytes;
  }

  /**
   * @return the node names that we prefer the Evaluator to run on
   */
  public List<String> getNodeNames() {
    return Collections.unmodifiableList(nodeNames);
  }

  /**
   * @return the rack names that we prefer the Evaluator to run on
   */
  public List<String> getRackNames() {
    return Collections.unmodifiableList(rackNames);
  }

  /**
   * {@link EvaluatorRequest}s are build using this Builder.
   */
  public static final class Builder implements org.apache.reef.util.Builder<EvaluatorRequest> {

    private int n = 1;
    @Deprecated
    private ResourceCatalog.Descriptor descriptor = null;
    private int megaBytes = -1;
    private int cores = 1; //if not set, default to 1
    private final List<String> nodeNames = new ArrayList<>();
    private final List<String> rackNames = new ArrayList<>();

    private Builder() {
    }

    private Builder(final EvaluatorRequest request) {
      setNumber(request.getNumber());
      fromDescriptor(request.getDescriptor());
    }

    /**
     * @param megaBytes the amount of megabytes to request for the Evaluator.
     * @return this builder
     */
    public Builder setMemory(final int megaBytes) {
      this.megaBytes = megaBytes;
      return this;
    }

    /**
     * set number of cores.
     *
     * @param cores the number of cores
     * @return
     */
    public Builder setNumberOfCores(final int cores) {
      this.cores = cores;
      return this;
    }

    /**
     * Set the number of Evaluators requested.
     *
     * @param n
     * @return this Builder.
     */
    public Builder setNumber(final int n) {
      this.n = n;
      return this;
    }

    /**
     * Add a node name.
     */
    public Builder addNodeName(final String nodeName) {
      this.nodeNames.add(nodeName);
      return this;
    }

    /**
     * Add a rack name.
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
      return new EvaluatorRequest(this.n, this.megaBytes, this.cores, this.nodeNames, this.rackNames);
    }

    /**
     * Pre-fill this {@link EvaluatorRequest} from the given
     * {@link org.apache.reef.driver.catalog.NodeDescriptor}. Any value not changed in subsequent calls to
     * this Builder will be taken from the given descriptor.
     *
     * @param rd the descriptor used to pre-fill this request.
     * @return this
     */
    @Deprecated
    public Builder fromDescriptor(final ResourceCatalog.Descriptor rd) {
      this.descriptor = rd;
      return this;
    }
  }
}
