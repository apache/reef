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
package org.apache.reef.vortex.examples.matmul;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.vortex.driver.VortexJobConf;
import org.apache.reef.vortex.driver.VortexLauncher;
import org.apache.reef.vortex.driver.VortexMasterConf;

/**
 * User's main function.
 */
final class MatMul {
  private MatMul() {
  }

  /**
   * Launch the vortex job, passing appropriate arguments.
   */
  public static void main(final String[] args) {
    final Configuration vortexMasterConf = VortexMasterConf.CONF
        .set(VortexMasterConf.WORKER_NUM, 2)
        .set(VortexMasterConf.WORKER_MEM, 1024)
        .set(VortexMasterConf.WORKER_CORES, 4)
        .set(VortexMasterConf.WORKER_CAPACITY, 2000)
        .set(VortexMasterConf.VORTEX_START, IdentityMatMulStart.class)
        .build();

    final Configuration userConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(DivideFactor.class, String.valueOf("10000"))
        .bindNamedParameter(NumRows.class, String.valueOf("100000"))
        .bindNamedParameter(NumColumns.class, String.valueOf("10"))
        .build();

    final VortexJobConf vortexJobConf = VortexJobConf.newBuilder()
        .setVortexMasterConf(vortexMasterConf)
        .setJobName("Vortex_Example_MatMul")
        .setUserConf(userConf)
        .build();

    VortexLauncher.launchLocal(vortexJobConf);
  }

  @NamedParameter(doc = "Number of splits the matrix is divided into")
  public static class DivideFactor implements Name<Integer> {
  }

  @NamedParameter(doc = "Number of rows of the original matrix")
  public static class NumRows implements Name<Integer> {
  }

  @NamedParameter(doc = "Number of columns of the original matrix")
  public static class NumColumns implements Name<Integer> {
  }
}
