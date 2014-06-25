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
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.io.TempFileCreator;
import com.microsoft.reef.io.WorkingDirectoryTempFileCreator;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.util.YarnConfigurationConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.time.Clock;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public final class YarnMasterConfiguration extends AbstractDriverRuntimeConfiguration {

  public final static String GLOBAL_FILE_DIRECTORY = "global";

  public YarnMasterConfiguration() {
    super(YARNResourceLaunchHandler.class,
        YARNResourceReleaseHandler.class,
        YarnResourceRequestHandler.class);
    try {
      this.builder.bindConstructor(YarnConfiguration.class, YarnConfigurationConstructor.class);
      this.builder.bindSetEntry(Clock.RuntimeStartHandler.class, YARNRuntimeStartHandler.class);
      this.builder.bindSetEntry(Clock.RuntimeStopHandler.class, YARNRuntimeStopHandler.class);
      this.builder.bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class);
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  // JOB SUBMISSION DIRECTORY
  @NamedParameter(doc = "The job submission directory.")
  public final static class JobSubmissionDirectory implements Name<String> {
  }

  // YarnConfiguration File
  @NamedParameter(doc = "The YARN configuration file name.")
  public final static class YarnConfigurationFile implements Name<String> {
  }

  // DUTY CYCLE
  @NamedParameter(doc = "How often we talk to YARN.", default_value = "1000")
  public final static class YarnHeartbeatPeriod implements Name<Integer> {
  }

  public final YarnMasterConfiguration setJobSubmissionDirectory(final String path) {
    try {
      this.builder.bindNamedParameter(JobSubmissionDirectory.class, path);
      return this;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  public final YarnMasterConfiguration setYarnConfigurationFile(final String file) {
    try {
      this.builder.bindNamedParameter(YarnConfigurationFile.class, file);
      return this;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }

  public final YarnMasterConfiguration setYarnHeartbeatPeriod(final Long period) {
    try {
      this.builder.bindNamedParameter(YarnHeartbeatPeriod.class, Long.toString(period));
      return this;
    } catch (final BindException e) {
      throw new RuntimeException(e);
    }
  }
}
