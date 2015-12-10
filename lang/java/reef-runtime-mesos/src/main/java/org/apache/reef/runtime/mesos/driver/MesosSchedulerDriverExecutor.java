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
package org.apache.reef.runtime.mesos.driver;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for Mesos SchedulerDriver.
 */
public class MesosSchedulerDriverExecutor implements EventHandler<SchedulerDriver> {
  private static final Logger LOG = Logger.getLogger(MesosSchedulerDriverExecutor.class.getName());

  @Inject
  public MesosSchedulerDriverExecutor() {
  }

  @Override
  public void onNext(final SchedulerDriver schedulerDriver) {
    LOG.log(Level.INFO, "MesosMaster(SchedulerDriver) starting");
    final Protos.Status status = schedulerDriver.run();
    LOG.log(Level.INFO, "MesosMaster(SchedulerDriver) ended with status {0}", status);
  }
}
