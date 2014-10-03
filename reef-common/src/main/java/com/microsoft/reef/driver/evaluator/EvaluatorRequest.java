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
package com.microsoft.reef.driver.evaluator;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.ResourceCatalog;

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
  private final ResourceCatalog.Descriptor descriptor;

  EvaluatorRequest(final int number,
                   final int megaBytes,
                   final int cores,
                   final ResourceCatalog.Descriptor descriptor) {
    this.number = number;
    this.megaBytes = megaBytes;
    this.cores = cores;
    this.descriptor = descriptor;
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
   * Access the {@link NodeDescriptor} used as the template for this
   * {@link EvaluatorRequest}.
   *
   * @return the {@link NodeDescriptor} used as the template for this
   * {@link EvaluatorRequest}.
   */
  public final ResourceCatalog.Descriptor getDescriptor() {
    return this.descriptor;
  }

  /**
   * @return the minimum size of Evaluator requested.
   */
  public int getMegaBytes() {
    return megaBytes;
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
   * {@link EvaluatorRequest}s are build using this Builder.
   */
  public static class Builder implements com.microsoft.reef.util.Builder<EvaluatorRequest> {

    private int n = 1;
    private ResourceCatalog.Descriptor descriptor = null;
    private int megaBytes = -1;
    private int cores = 1; //if not set, default to 1

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
     * set number of cores
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
     * Builds the {@link EvaluatorRequest}.
     */
    @Override
    public EvaluatorRequest build() {
      return new EvaluatorRequest(this.n, this.megaBytes, this.cores, this.descriptor);
    }

    /**
     * Pre-fill this {@link EvaluatorRequest} from the given
     * {@link NodeDescriptor}. Any value not changed in subsequent calls to
     * this Builder will be taken from the given descriptor.
     *
     * @param rd the descriptor used to pre-fill this request.
     * @return this
     */
    public Builder fromDescriptor(final ResourceCatalog.Descriptor rd) {
      this.descriptor = rd;
      return this;
    }
  }
}
