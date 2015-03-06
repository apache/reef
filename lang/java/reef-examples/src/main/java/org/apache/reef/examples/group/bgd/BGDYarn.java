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
package org.apache.reef.examples.group.bgd;

import org.apache.reef.client.LauncherStatus;
import org.apache.reef.examples.group.utils.timer.Timer;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runs BGD on the YARN runtime.
 */
public class BGDYarn {

  private static final Logger LOG = Logger.getLogger(BGDYarn.class.getName());

  private static final int TIMEOUT = 4 * Timer.HOURS;

  public static void main(final String[] args) throws Exception {

    final BGDClient bgdClient = BGDClient.fromCommandLine(args);

    final Configuration runtimeConfiguration = YarnClientConfiguration.CONF
        .set(YarnClientConfiguration.JVM_HEAP_SLACK, "0.1")
        .build();

    final String jobName = System.getProperty("user.name") + "-" + "BR-ResourceAwareBGD-YARN";

    final LauncherStatus status = bgdClient.run(runtimeConfiguration, jobName, TIMEOUT);

    LOG.log(Level.INFO, "OUT: Status = {0}", status);
  }
}
