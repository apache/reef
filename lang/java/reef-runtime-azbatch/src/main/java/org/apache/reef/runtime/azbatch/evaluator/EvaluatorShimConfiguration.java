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
package org.apache.reef.runtime.azbatch.evaluator;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.parameters.ContainerIdentifier;
import org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.remote.RemoteConfiguration;

/**
 * ConfigurationModule to create evaluator shim configurations.
 */
@Private
@EvaluatorSide
public class EvaluatorShimConfiguration extends ConfigurationModuleBuilder {

  /**
   * @see org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier
   */
  public static final RequiredParameter<String> DRIVER_REMOTE_IDENTIFIER = new RequiredParameter<>();

  /**
   * @see org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier
   */
  public static final RequiredParameter<String> CONTAINER_IDENTIFIER = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new EvaluatorShimConfiguration()
      .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
      .bindNamedParameter(DriverRemoteIdentifier.class, DRIVER_REMOTE_IDENTIFIER)
      .bindNamedParameter(ContainerIdentifier.class, CONTAINER_IDENTIFIER)
      .build();
}
