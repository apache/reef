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
package com.microsoft.reef.runtime.yarn.util;

import com.microsoft.reef.runtime.yarn.driver.YarnMasterConfiguration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.inject.Inject;

public final class YarnConfigurationConstructor implements ExternalConstructor<YarnConfiguration> {

  private final YarnConfiguration conf;

  @Inject
  YarnConfigurationConstructor() {
    this.conf = new YarnConfiguration();
  }

  @Inject
  YarnConfigurationConstructor(@Parameter(YarnMasterConfiguration.YarnConfigurationFile.class) final String file) {
    this();
    this.conf.addResource(new Path(file));
  }

  @Override
  public YarnConfiguration newInstance() {
    return this.conf;
  }
}
