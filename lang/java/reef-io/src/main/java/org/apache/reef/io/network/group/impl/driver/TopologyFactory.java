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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.io.network.group.api.driver.Topology;
import org.apache.reef.io.network.group.impl.config.parameters.OperatorNameClass;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;

/**
 * A factory used to create Topology instance.
 * Uses Tang to instantiate new object.
 */
public final class TopologyFactory {

  private final Injector injector;

  @Inject
  private TopologyFactory(final Injector injector) {
    this.injector = injector;
  }

  /**
   * Instantiates a new Topology instance.
   * @param operatorName specified name of the operator
   * @param topologyClass specified topology type
   * @return Topology instance
   * @throws InjectionException
   */
  public Topology getNewInstance(final Class<? extends Name<String>> operatorName,
                                 final Class<? extends Topology> topologyClass) throws InjectionException {
    final Injector newInjector = injector.forkInjector();
    newInjector.bindVolatileParameter(OperatorNameClass.class, operatorName);
    return newInjector.getInstance(topologyClass);
  }
}
