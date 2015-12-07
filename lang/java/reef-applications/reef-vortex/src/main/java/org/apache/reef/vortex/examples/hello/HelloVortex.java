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
package org.apache.reef.vortex.examples.hello;

import org.apache.reef.tang.Configuration;
import org.apache.reef.vortex.driver.VortexJobConf;
import org.apache.reef.vortex.driver.VortexLauncher;
import org.apache.reef.vortex.driver.VortexMasterConf;

/**
 * User's main function.
 */
final class HelloVortex {
  private HelloVortex() {
  }

  /**
   * Launch the vortex job, passing appropriate arguments.
   */
  public static void main(final String[] args) {
    final Configuration vortexMasterConf = VortexMasterConf.CONF
        .set(VortexMasterConf.WORKER_NUM, 1)
        .set(VortexMasterConf.WORKER_MEM, 1024)
        .set(VortexMasterConf.WORKER_CORES, 1)
        .set(VortexMasterConf.WORKER_CAPACITY, 2000)
        .set(VortexMasterConf.VORTEX_START, HelloVortexStart.class)
        .build();

    final VortexJobConf vortexJobConf = VortexJobConf.newBuilder()
        .setVortexMasterConf(vortexMasterConf)
        .setJobName("HelloVortex")
        .build();

    VortexLauncher.launchLocal(vortexJobConf);
  }
}
