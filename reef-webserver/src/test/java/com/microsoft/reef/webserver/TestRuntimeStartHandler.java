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

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.runtime.RuntimeClock;
import com.microsoft.wake.time.runtime.event.RuntimeStart;
import com.microsoft.wake.time.runtime.event.RuntimeStop;
import org.junit.*;

import java.util.Set;

/**
 * TestRuntimeStartHandler
 */
public class TestRuntimeStartHandler {
  /**
   * With HttpHandlerConfiguration merged with HttpRuntimeConfiguration and binding for http handlers, when inject RuntimeClock
   * all the nested objects including HeetServer, JettyHandler, HttpRuntimeStartHandler and  HttpRuntimeStopHandler
   *
   * @throws BindException
   * @throws InjectionException
   */
  @Test
  public void testHttpHandlerBindingWithRuntimeClock() throws BindException, InjectionException {
   final Configuration clockConfiguraiton = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();
    final RuntimeClock clock = Tang.Factory.getTang().newInjector(clockConfiguraiton).getInstance(RuntimeClock.class);
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
    final Configuration clockConfiguraiton = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(clockConfiguraiton);
    final Set<EventHandler<RuntimeStart>> startEventHandlers = injector.getNamedInstance(RuntimeClock.RuntimeStartHandler.class);
    for (final EventHandler<RuntimeStart> enventHandler : startEventHandlers) {
      final HttpRuntimeStartHandler runtimeStartHandler = (HttpRuntimeStartHandler)enventHandler;
      try {
        runtimeStartHandler.onNext(null);
      } catch (final Exception e) {
        throw new RuntimeException("Fail to call onNext on runtimeStartHandler.", e);
      }
    }

    final Set<EventHandler<RuntimeStop>> stopEventHandlers = injector.getNamedInstance(RuntimeClock.RuntimeStopHandler.class);
    for (final EventHandler<RuntimeStop> enventHandler : stopEventHandlers) {
      final HttpRuntimeStopHandler runtimeStopHandler = (HttpRuntimeStopHandler)enventHandler;
      try {
        runtimeStopHandler.onNext(null);
      } catch (final Exception e) {
        throw new RuntimeException("Fail to call onNext on runtimeStopHandler.", e);
      }
    }
  }
}
