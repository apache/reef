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
package org.apache.reef.examples.group.bgd.parameters;

import org.apache.reef.examples.group.bgd.loss.LossFunction;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Control parameters for BGD example.
 */
public final class BGDControlParameters {

  private final int dimensions;
  private final double lambda;
  private final double eps;
  private final int iters;
  private final int minParts;
  private final boolean rampup;

  private final double eta;
  private final double probOfSuccessfulIteration;
  private final BGDLossType lossType;

  @Inject
  public BGDControlParameters(
      @Parameter(ModelDimensions.class) final int dimensions,
      @Parameter(Lambda.class) final double lambda,
      @Parameter(Eps.class) final double eps,
      @Parameter(Eta.class) final double eta,
      @Parameter(ProbabilityOfSuccessfulIteration.class) final double probOfSuccessfulIteration,
      @Parameter(Iterations.class) final int iters,
      @Parameter(EnableRampup.class) final boolean rampup,
      @Parameter(MinParts.class) final int minParts,
      final BGDLossType lossType) {
    this.dimensions = dimensions;
    this.lambda = lambda;
    this.eps = eps;
    this.eta = eta;
    this.probOfSuccessfulIteration = probOfSuccessfulIteration;
    this.iters = iters;
    this.rampup = rampup;
    this.minParts = minParts;
    this.lossType = lossType;
  }

  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ModelDimensions.class, Integer.toString(this.dimensions))
        .bindNamedParameter(Lambda.class, Double.toString(this.lambda))
        .bindNamedParameter(Eps.class, Double.toString(this.eps))
        .bindNamedParameter(Eta.class, Double.toString(this.eta))
        .bindNamedParameter(ProbabilityOfSuccessfulIteration.class, Double.toString(probOfSuccessfulIteration))
        .bindNamedParameter(Iterations.class, Integer.toString(this.iters))
        .bindNamedParameter(EnableRampup.class, Boolean.toString(this.rampup))
        .bindNamedParameter(MinParts.class, Integer.toString(this.minParts))
        .bindNamedParameter(LossFunctionType.class, lossType.lossFunctionString())
        .build();
  }

  public static CommandLine registerShortNames(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(ModelDimensions.class)
        .registerShortNameOfClass(Lambda.class)
        .registerShortNameOfClass(Eps.class)
        .registerShortNameOfClass(Eta.class)
        .registerShortNameOfClass(ProbabilityOfSuccessfulIteration.class)
        .registerShortNameOfClass(Iterations.class)
        .registerShortNameOfClass(EnableRampup.class)
        .registerShortNameOfClass(MinParts.class)
        .registerShortNameOfClass(LossFunctionType.class);
  }

  public int getDimensions() {
    return this.dimensions;
  }

  public double getLambda() {
    return this.lambda;
  }

  public double getEps() {
    return this.eps;
  }

  public double getEta() {
    return this.eta;
  }

  public double getProbOfSuccessfulIteration() {
    return probOfSuccessfulIteration;
  }

  public int getIters() {
    return this.iters;
  }

  public int getMinParts() {
    return this.minParts;
  }

  public boolean isRampup() {
    return this.rampup;
  }

  public Class<? extends LossFunction> getLossFunction() {
    return this.lossType.getLossFunction();
  }
}
