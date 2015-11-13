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
package org.apache.reef.webserver;

import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.time.runtime.RuntimeClock;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Set;

/**
 * TestRuntimeStartHandler.
 */
public class TestRuntimeStartHandler {

  private Configuration configuration;

  @Before
  public void setUp() throws InjectionException, IOException, ServletException {
    final Configuration clockConfiguration = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();
    final Configuration remoteConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_TEST_REMOTE_MANAGER")
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .bindNamedParameter(JobIdentifier.class, "my job")
        .build();
    this.configuration = Configurations.merge(clockConfiguration, remoteConfiguration);
  }

  /**
   * With HttpHandlerConfiguration merged with HttpRuntimeConfiguration and binding for http handlers,
   * when inject RuntimeClock.
   * all the nested objects including HttpServer, JettyHandler, HttpRuntimeStartHandler and  HttpRuntimeStopHandler
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testHttpHandlerBindingWithRuntimeClock() throws BindException, InjectionException {
    final RuntimeClock clock = Tang.Factory.getTang().newInjector(this.configuration).getInstance(RuntimeClock.class);
    Assert.assertNotNull(clock);
  }

  /**
   * This test is to get RuntimeStartHandler, simulate the call to onNext to so tah tto start Jetty server.
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testRunTimeStartStopHandler() throws BindException, InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(this.configuration);
    final Set<EventHandler<RuntimeStart>> startEventHandlers =
        injector.getNamedInstance(RuntimeClock.RuntimeStartHandler.class);
    for (final EventHandler<RuntimeStart> eventHandler : startEventHandlers) {
      final HttpRuntimeStartHandler runtimeStartHandler = (HttpRuntimeStartHandler) eventHandler;
      try {
        runtimeStartHandler.onNext(null);
      } catch (final Exception e) {
        throw new RuntimeException("Fail to call onNext on runtimeStartHandler.", e);
      }
    }

    final Set<EventHandler<RuntimeStop>> stopEventHandlers =
        injector.getNamedInstance(RuntimeClock.RuntimeStopHandler.class);
    for (final EventHandler<RuntimeStop> eventHandler : stopEventHandlers) {
      final HttpRuntimeStopHandler runtimeStopHandler = (HttpRuntimeStopHandler) eventHandler;
      try {
        runtimeStopHandler.onNext(null);
      } catch (final Exception e) {
        throw new RuntimeException("Fail to call onNext on runtimeStopHandler.", e);
      }
    }
  }
}
