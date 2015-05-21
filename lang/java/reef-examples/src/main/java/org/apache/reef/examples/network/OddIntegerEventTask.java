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
package org.apache.reef.examples.network;

import org.apache.reef.io.network.NetworkService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.io.network.util.DriverIDProvider;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Starts to exchange IntegerEvent and waits to finish after sends String message from driver
 */
public final class OddIntegerEventTask implements Task {
  private static final Logger LOG = Logger.getLogger(OddIntegerEventTask.class.getName());

  private final NetworkService nsService;
  private final Monitor monitor;
  private final Identifier secondEvaluatorId;
  private final DriverIDProvider driverIDProvider;
  private final MonitorForDriver monitorForDriver;

  @Inject
  public OddIntegerEventTask(
      final NetworkService nsService,
      final MonitorForDriver monitorForDriver,
      final Monitor monitor,
      final DriverIDProvider driverIDProvider,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      @Parameter(SecondNetworkServiceId.class) final String secondServiceIdString) {
    this.monitor = monitor;
    this.driverIDProvider = driverIDProvider;
    this.monitorForDriver = monitorForDriver;
    this.nsService = nsService;
    this.secondEvaluatorId = idFactory.getNewInstance(secondServiceIdString);
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    nsService.sendEvent(driverIDProvider.getId(), "Hello, ");
    LOG.log(Level.INFO, "waits for message from driver");
    monitorForDriver.monitorWait();
    LOG.log(Level.INFO, "start to exchange events");
    nsService.sendEvent(secondEvaluatorId, new IntegerEvent(0));
    monitor.monitorWait();
    return null;
  }

  @NamedParameter(doc = "second evaluator network service id")
  public static final class SecondNetworkServiceId implements Name<String> {
  }
}
