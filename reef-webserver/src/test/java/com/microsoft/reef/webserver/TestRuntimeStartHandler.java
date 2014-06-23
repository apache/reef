/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.webserver;

import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteConfiguration;
import com.microsoft.wake.time.runtime.RuntimeClock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Set;

/**
 * TestRuntimeStartHandler
 */
public class TestRuntimeStartHandler {

  private Configuration configuation;

  @Before
  public void setUp() throws InjectionException, IOException, ServletException {
    final Configuration clockConfiguraiton = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();
    final Configuration remoteConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_TEST_REMOTE_MANAGER")
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, "my job")
        .build();
    this.configuation = Configurations.merge(clockConfiguraiton, remoteConfiguration);
  }

  /**
   * With HttpHandlerConfiguration merged with HttpRuntimeConfiguration and binding for http handlers, when inject RuntimeClock
   * all the nested objects including HeetServer, JettyHandler, HttpRuntimeStartHandler and  HttpRuntimeStopHandler
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testHttpHandlerBindingWithRuntimeClock() throws BindException, InjectionException {
    final RuntimeClock clock = Tang.Factory.getTang().newInjector(this.configuation).getInstance(RuntimeClock.class);
    Assert.assertNotNull(clock);
  }

  /**
   * This test is to get RuntimeStartHandler, simulate the call to onNext to so tah tto start Jetty server
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testRunTimeStartStopHandler() throws BindException, InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(this.configuation);
    final Set<EventHandler<RuntimeStart>> startEventHandlers = injector.getNamedInstance(RuntimeClock.RuntimeStartHandler.class);
    for (final EventHandler<RuntimeStart> enventHandler : startEventHandlers) {
      final HttpRuntimeStartHandler runtimeStartHandler = (HttpRuntimeStartHandler) enventHandler;
      try {
        runtimeStartHandler.onNext(null);
      } catch (final Exception e) {
        throw new RuntimeException("Fail to call onNext on runtimeStartHandler.", e);
      }
    }

    final Set<EventHandler<RuntimeStop>> stopEventHandlers = injector.getNamedInstance(RuntimeClock.RuntimeStopHandler.class);
    for (final EventHandler<RuntimeStop> enventHandler : stopEventHandlers) {
      final HttpRuntimeStopHandler runtimeStopHandler = (HttpRuntimeStopHandler) enventHandler;
      try {
        runtimeStopHandler.onNext(null);
      } catch (final Exception e) {
        throw new RuntimeException("Fail to call onNext on runtimeStopHandler.", e);
      }
    }
  }
}
