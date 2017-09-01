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
package org.apache.reef.runtime.yarn.util;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.runtime.yarn.driver.parameters.FileSystemUrl;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An external constructor that creates YarnConfiguration instances.
 */
public final class YarnConfigurationConstructor implements ExternalConstructor<YarnConfiguration> {

  private static final Logger LOG = Logger.getLogger(YarnConfigurationConstructor.class.getName());

  private final String fileSystemUrl;

  @Inject
  YarnConfigurationConstructor(@Parameter(FileSystemUrl.class) final String fileSystemUrl) {
    this.fileSystemUrl = fileSystemUrl;
  }

  @Override
  public YarnConfiguration newInstance() {
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    if (!fileSystemUrl.equals(FileSystemUrl.DEFAULT_VALUE)) {
      yarnConfiguration.set("fs.defaultFS", fileSystemUrl);
      LOG.log(Level.INFO, "Set fileSystemUrl in YarnConfigurationConstructor: {0}", fileSystemUrl);
    }
    return yarnConfiguration;
  }
}
