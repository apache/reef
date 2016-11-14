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
package org.apache.reef.examples.group.bgd.utils;

import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.examples.group.bgd.MasterTask;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Subconfiguration: limit given configuration to a given list of classes.
 */
public final class SubConfiguration {

  private static final Logger LOG = Logger.getLogger(SubConfiguration.class.getName());

  @SafeVarargs
  public static Configuration from(
      final Configuration baseConf, final Class<? extends Name<?>>... classes) {

    final Injector injector = Tang.Factory.getTang().newInjector(baseConf);
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();

    for (final Class<? extends Name<?>> clazz : classes) {
      try {
        confBuilder.bindNamedParameter(clazz,
            injector.getNamedInstance((Class<? extends Name<Object>>) clazz).toString());
      } catch (final InjectionException ex) {
        final String msg = "Exception while creating subconfiguration";
        LOG.log(Level.WARNING, msg, ex);
        throw new RuntimeException(msg, ex);
      }
    }

    return confBuilder.build();
  }

  public static void main(final String[] args) throws InjectionException {

    final Configuration conf = TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, "TASK")
        .set(TaskConfiguration.TASK, MasterTask.class)
        .build();

    final Configuration subConf = SubConfiguration.from(conf, TaskConfigurationOptions.Identifier.class);
    LOG.log(Level.INFO, "OUT: Base conf:\n{0}", Configurations.toString(conf));
    LOG.log(Level.INFO, "OUT: Sub conf:\n{0}", Configurations.toString(subConf));
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private SubConfiguration() {
  }
}
