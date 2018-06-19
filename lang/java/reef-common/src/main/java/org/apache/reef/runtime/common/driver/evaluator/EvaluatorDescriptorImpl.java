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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorProcess;

import javax.inject.Inject;

/**
 * A simple all-data implementation of EvaluatorDescriptor.
 */
@Private
@DriverSide
final class EvaluatorDescriptorImpl implements EvaluatorDescriptor {

  private final NodeDescriptor nodeDescriptor;
  private final int megaBytes;
  private final int numberOfCores;
  private EvaluatorProcess process;
  private final String runtimeName;

  private EvaluatorDescriptorImpl(
      final NodeDescriptor nodeDescriptor,
      final int megaBytes,
      final int numberOfCores,
      final EvaluatorProcess process,
      final String runtimeName) {
    this.nodeDescriptor = nodeDescriptor;
    this.megaBytes = megaBytes;
    this.numberOfCores = numberOfCores;
    this.process = process;
    this.runtimeName = runtimeName;
  }

  @Override
  public NodeDescriptor getNodeDescriptor() {
    return this.nodeDescriptor;
  }

  @Override
  public synchronized EvaluatorProcess getProcess() {
    if (null == this.process) {
      throw new IllegalArgumentException("EvaluatorProcess must be set");
    }
    return this.process;
  }

  public synchronized void setProcess(final EvaluatorProcess process) {
    this.process = process;
  }

  @Override
  public int getMemory() {
    return this.megaBytes;
  }

  @Override
  public int getNumberOfCores() {
    return this.numberOfCores;
  }

  @Override
  public String getRuntimeName() {
    return this.runtimeName;
  }

  /**
   * Evaluator descriptor builder factory that creates a  new evaluator descriptor builder impl.
   */
  static final class BuilderFactory implements EvaluatorDescriptorBuilderFactory {
    @Inject
    private BuilderFactory() {
    }

    @Override
    public EvaluatorDescriptorBuilder newBuilder() {
      return new Builder();
    }

    @Override
    public EvaluatorDescriptorBuilder newBuilder(final EvaluatorDescriptor copy) {
      return newBuilder()
          .setNodeDescriptor(copy.getNodeDescriptor())
          .setMemory(copy.getMemory())
          .setNumberOfCores(copy.getNumberOfCores())
          .setEvaluatorProcess(copy.getProcess())
          .setRuntimeName(copy.getRuntimeName());
    }
  }

  /**
   * An builder for this evaluator descriptor implementation.
   */
  private static final class Builder implements EvaluatorDescriptorBuilder {
    private NodeDescriptor nodeDescriptor = null;
    private int memory = 0;
    private int numberOfCores = 0;
    private EvaluatorProcess evaluatorProcess = null;
    private String runtimeName = null;

    @Override
    public Builder setNodeDescriptor(final NodeDescriptor nodeDescriptor) {
      this.nodeDescriptor = nodeDescriptor;
      return this;
    }

    @Override
    public Builder setMemory(final int megaBytes) {
      this.memory = megaBytes;
      return this;
    }

    @Override
    public Builder setNumberOfCores(final int numberOfCores) {
      this.numberOfCores = numberOfCores;
      return this;
    }

    @Override
    public Builder setEvaluatorProcess(final EvaluatorProcess evaluatorProcess) {
      this.evaluatorProcess = evaluatorProcess;
      return this;
    }

    @Override
    public Builder setRuntimeName(final String runtimeName) {
      this.runtimeName = runtimeName;
      return this;
    }

    @Override
    public EvaluatorDescriptor build() {
      if (this.memory == 0) {
        throw new IllegalArgumentException("memory not set");
      } else if (this.numberOfCores == 0) {
        throw new IllegalArgumentException("number of cores not set");
      }
      return new EvaluatorDescriptorImpl(
          this.nodeDescriptor,
          this.memory,
          this.numberOfCores,
          this.evaluatorProcess,
          this.runtimeName);
    }
  }
}
