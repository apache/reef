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
package org.apache.reef.bridge.driver.service;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.service.parameters.DriverClientCommand;
import org.apache.reef.driver.parameters.DriverIdleSources;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Binds all driver bridge service handlers to the driver.
 */
@Private
public final class DriverServiceConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredImpl<IDriverService> DRIVER_SERVICE_IMPL = new RequiredImpl<>();

  public static final RequiredParameter<String> DRIVER_CLIENT_COMMAND = new RequiredParameter<>();

  /** Configuration module that binds all driver handlers. */
  public static final ConfigurationModule CONF = new DriverServiceConfiguration()
      .bindImplementation(IDriverService.class, DRIVER_SERVICE_IMPL)
      .bindNamedParameter(DriverClientCommand.class, DRIVER_CLIENT_COMMAND)
      .bindSetEntry(DriverIdleSources.class, IDriverService.class)
      .build();
}
