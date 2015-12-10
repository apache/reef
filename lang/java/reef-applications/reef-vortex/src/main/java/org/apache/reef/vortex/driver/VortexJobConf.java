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
package org.apache.reef.vortex.driver;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.util.BuilderUtils;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.Optional;

/**
 * Helper class for building a configuration for Vortex.
 */
@Unstable
public final class VortexJobConf {
  private final String jobName;
  private final Configuration vortexMasterConf;
  private final Optional<Configuration> userConf;

  private VortexJobConf(final String jobName,
                        final Configuration vortexMasterConf,
                        final Optional<Configuration> userConf) {
    this.jobName = jobName;
    this.vortexMasterConf = vortexMasterConf;
    this.userConf = userConf;
  }

  /**
   * Create a Builder object for Vortex job configuration.
   */
  public static Builder newBuilder() {
    return new VortexJobConf.Builder();
  }

  /**
   * Convert to the Tang Configuration.
   */
  @Private
  public Configuration getConfiguration() {
    final Configuration vortexDriverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(VortexDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, VortexDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, VortexDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, VortexDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, VortexDriver.TaskMessageHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, VortexDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName)
        .build();

    if (userConf.isPresent()) {
      return Configurations.merge(vortexDriverConf, vortexMasterConf, userConf.get());
    } else {
      return Configurations.merge(vortexDriverConf, vortexMasterConf);
    }
  }

  /**
   * Builder object to create a {@link VortexJobConf}.
   */
  public static final class Builder implements org.apache.reef.util.Builder<VortexJobConf> {
    private String jobName;
    private Configuration vortexMasterConf;
    private Optional<Configuration> userConf = Optional.empty();

    private Builder() {
    }

    /**
     * @param vortexMasterConf Configuration for the Vortex Master, which can be built via {@link VortexMasterConf}.
     */
    public Builder setVortexMasterConf(final Configuration vortexMasterConf) {
      this.vortexMasterConf = vortexMasterConf;
      return this;
    }

    /**
     * @param userConf Configuration set by user (e.g., Parameters in {@link org.apache.reef.vortex.api.VortexStart}
     */
    public Builder setUserConf(final Configuration userConf) {
      this.userConf = Optional.of(userConf);
      return this;
    }

    /**
     * @param jobName Name of the job which is assigned to the Driver.
     */
    public Builder setJobName(final String jobName) {
      this.jobName = jobName;
      return this;
    }

    @Override
    public VortexJobConf build() {
      BuilderUtils.notNull(jobName);
      BuilderUtils.notNull(vortexMasterConf);
      return new VortexJobConf(jobName, vortexMasterConf, userConf);
    }
  }
}
