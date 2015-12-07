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
package org.apache.reef.vortex.examples.addone;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.vortex.driver.VortexConfHelper;
import org.apache.reef.vortex.driver.VortexLauncher;

/**
 * User's main function.
 */
final class AddOne {
  private AddOne() {
  }

  /**
   * Launch the vortex job, passing appropriate arguments.
   */
  public static void main(final String[] args) {
    final Configuration vortexConf =
        Configurations.merge(
            VortexConfHelper.getVortexConf("Vortex_Example_AddOne", AddOneStart.class, 2, 1024, 4, 2000),
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(Dimension.class, "1000")
                .build());
    VortexLauncher.launchLocal(vortexConf);
  }

  @NamedParameter(doc = "dimension of input vector")
  public static class Dimension implements Name<Integer> {
  }
}
