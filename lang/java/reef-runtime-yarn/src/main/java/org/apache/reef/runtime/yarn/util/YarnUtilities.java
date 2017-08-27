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

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.reef.annotations.audience.Private;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A helper class for YARN applications.
 */
@Private
public final class YarnUtilities {
  public static final String REEF_YARN_APPLICATION_ID_ENV_VAR = "REEF_YARN_APPLICATION_ID";
  public static final String REEF_YARN_NODE_LABEL_ID = "REEF_YARN_NODE_LABEL_ID";
  private static final Logger LOG = Logger.getLogger(YarnUtilities.class.getName());

  /**
   * @return the Container ID of the running Container.
   */
  public static String getContainerIdString() {
    try {
      return System.getenv(ApplicationConstants.Environment.CONTAINER_ID.key());
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Unable to get the container ID from the environment, exception " +
          e + " was thrown.");
      return null;
    }
  }

  /**
   * @return the Application ID of the YARN application.
   */
  public static ApplicationId getApplicationId() {
    if (getAppAttemptId() == null) {
      return null;
    }

    return getAppAttemptId().getApplicationId();
  }

  /**
   * @return the Application Attempt ID of the YARN application.
   */
  public static ApplicationAttemptId getAppAttemptId() {
    return getAppAttemptId(getContainerIdString());
  }

  /**
   * @param containerIdString the Container ID of the running Container.
   * @return the Application Attempt ID of the YARN application.
   */
  public static ApplicationAttemptId getAppAttemptId(final String containerIdString) {
    if (containerIdString == null) {
      return null;
    }

    try {
      final ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
      return containerId.getApplicationAttemptId();
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Unable to get the applicationAttempt ID from the environment, exception " +
          e + " was thrown.");
      return null;
    }
  }

  private YarnUtilities() {
  }
}
