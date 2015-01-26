/**
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

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.reef.annotations.audience.Private;

import java.util.List;
import java.util.Map;

/**
 * Helper class that creates the various records in the YARN API
 */
@Private
public final class YarnTypes {

  private YarnTypes() {
  }

  /**
   * @return a ContainerLaunchContext with the given commands and LocalResources.
   */
  public static final ContainerLaunchContext getContainerLaunchContext(
      final List<String> commands, final Map<String, LocalResource> localResources) {
    final ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
    context.setLocalResources(localResources);
    context.setCommands(commands);
    return context;
  }

  public static boolean isAtOrAfterVersion(final String version) {
    final String hadoopVersion = VersionInfo.getVersion();

    if (hadoopVersion == null || hadoopVersion.length() < version.length()) {
      throw new RuntimeException("unsupported or incomplete hadoop version number provided for comparison: " + hadoopVersion);
    }

    return hadoopVersion.substring(0, version.length()).compareTo(version) >= 0;
  }
}
