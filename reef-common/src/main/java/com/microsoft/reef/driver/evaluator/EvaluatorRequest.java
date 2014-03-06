/**
 * Copyright (C) 2013 Microsoft Corporation
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
import com.microsoft.reef.driver.capabilities.Capability;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.catalog.ResourceCatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * A request for one ore more Evaluators.
 */
@Public
@DriverSide
@Provided
public final class EvaluatorRequest {

  /**
   * The size of the Evaluators requested.
   *
   * @deprecated in version 0.2. Use explicit memory requests instead.
   */
  @Deprecated
  public static enum Size {
    SMALL,
    MEDIUM,
    LARGE,
    XLARGE
  }

  private final Size size;
  private final int megaBytes;
  private final int number;
  private final List<Capability> capabilities;
  private final ResourceCatalog.Descriptor descriptor;

  EvaluatorRequest(final Size size,
                   final int number,
                   final int megaBytes,
                   final List<Capability> capabilities,
                   final ResourceCatalog.Descriptor descriptor) {
    this.size = size;
    this.number = number;
    this.megaBytes = megaBytes;
    this.capabilities = capabilities;
    this.descriptor = descriptor;
  }

  /**
   * Access the size of the evaluator requested.
   *
   * @return the size of the evaluator requested.
   * @deprecated in version 0.2. Use explicit memory requests instead.
   */
  @Deprecated
  public Size getSize() {
    return this.size;
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
   * Access the list of {@link Capability}s requested.
   *
   * @return the list of {@link Capability}s requested.
   */
  public final List<Capability> getCapabilities() {
    return this.capabilities;
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

    private Size evaluatorSize;
    private int n = 1;
    private final List<Capability> capabilities = new ArrayList<>();
    private ResourceCatalog.Descriptor descriptor = null;
    private int megaBytes = -1;

    private Builder() {
    }

    private Builder(final EvaluatorRequest request) {
      setSize(request.getSize());
      for (Capability capability : request.getCapabilities()) {
        withCapability(capability);
      }
      setNumber(request.getNumber());
      fromDescriptor(request.getDescriptor());
    }

    /**
     * Set the {@link Size} of the requested Evaluators.
     *
     * @param size
     * @return this builder
     * @deprecated in version 0.2. Use explicit memory requests instead (setMemory).
     */
    @Deprecated
    public Builder setSize(final Size size) {
      this.evaluatorSize = size;
      return this;
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
      return new EvaluatorRequest(this.evaluatorSize, this.n, this.megaBytes, this.capabilities, this.descriptor);
    }

    /**
     * Add a {@link Capability} to the request. If the capability cannot be
     * met by the {@link EvaluatorFactory}, the request will be denied.
     *
     * @param cap the {@link Capability} requested.
     * @return this Builder.
     */
    public Builder withCapability(final Capability cap) {
      this.capabilities.add(cap);
      return this;
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
