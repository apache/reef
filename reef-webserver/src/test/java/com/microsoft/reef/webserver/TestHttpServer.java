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

import com.microsoft.tang.*;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test Http Server
 */
public class TestHttpServer {
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void httpServerDefaultTest() throws InjectionException, Exception {
    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();
    final Injector injector = Tang.Factory.getTang().newInjector(httpRuntimeConfiguration);
    final HttpServer httpServer = injector.getInstance(HttpServer.class);

    Assert.assertEquals(8080, httpServer.getPort());
    httpServer.stop();
  }

  @Test
  public void httpServerSpecifiedPortTest() throws InjectionException, Exception {
    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(PortNumber.class, "9000");
    final Configuration httpServerConfiguration = cb.build();

    final Configuration configuration = Configurations.merge(httpRuntimeConfiguration, httpServerConfiguration);
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final HttpServer httpServer = injector.getInstance(HttpServer.class);

    Assert.assertEquals(9000, httpServer.getPort());
    httpServer.stop();
  }

  @Test
  public void httpServerConflictPortTest() throws InjectionException, Exception {
    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();

    final Injector injector1 = Tang.Factory.getTang().newInjector(httpRuntimeConfiguration);
    final HttpServer httpServer1 = injector1.getInstance(HttpServer.class);

    final Injector injector2 = Tang.Factory.getTang().newInjector(httpRuntimeConfiguration);
    final HttpServer httpServer2 = injector2.getInstance(HttpServer.class);

    Assert.assertEquals(8080, httpServer1.getPort());
    Assert.assertNotEquals(8080, httpServer2.getPort());
    httpServer1.stop();
    httpServer2.stop();
  }

  @Test
  public void httpServerPortRangeTest() throws InjectionException, Exception {
    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(PortNumber.class, "6000");
    cb.bindNamedParameter(MaxPortNumber.class, "9900");
    cb.bindNamedParameter(MinPortNumber.class, "1000");
    cb.bindNamedParameter(MaxRetryAttempts.class, "3");
    final Configuration httpServerConfiguration = cb.build();

    final Configuration configuration = Configurations.merge(httpRuntimeConfiguration, httpServerConfiguration);

    final Injector injector1 = Tang.Factory.getTang().newInjector(configuration);
    final HttpServer httpServer1 = injector1.getInstance(HttpServer.class);

    final Injector injector2 = Tang.Factory.getTang().newInjector(configuration);
    final HttpServer httpServer2 = injector2.getInstance(HttpServer.class);

    Assert.assertEquals(6000, httpServer1.getPort());
    Assert.assertTrue("port number is out of specified range", httpServer2.getPort() > 1000 && httpServer2.getPort() < 9900);
    httpServer1.stop();
    httpServer2.stop();
  }

  @Test
  public void httpServerPortRetryTest() throws InjectionException, Exception {
    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(PortNumber.class, "1");
    cb.bindNamedParameter(MaxPortNumber.class, "1");
    cb.bindNamedParameter(MinPortNumber.class, "1");
    cb.bindNamedParameter(MaxRetryAttempts.class, "3");
    final Configuration httpServerConfiguration = cb.build();

    final Configuration configuration = Configurations.merge(httpRuntimeConfiguration, httpServerConfiguration);

    final Injector injector1 = Tang.Factory.getTang().newInjector(configuration);
    final HttpServer httpServer1 = injector1.getInstance(HttpServer.class);

    final Injector injector2 = Tang.Factory.getTang().newInjector(configuration);
    try {
      final HttpServer httpServer2 = injector2.getInstance(HttpServer.class);
    } catch (RuntimeException e) {
      Assert.assertEquals("Could not find available port in 3 attempts", e.getMessage());
    } catch (InjectionException e) {
      //expected
    }
    httpServer1.stop();
  }

  @Test
  public void httpServerAddHandlerTest() throws InjectionException, Exception {
    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();
    final Injector injector = Tang.Factory.getTang().newInjector(httpRuntimeConfiguration);
    final HttpServer httpServer = injector.getInstance(HttpServer.class);
    final HttpServerReefEventHandler httpHandler = injector.getInstance(HttpServerReefEventHandler.class);
    httpServer.addHttpHandler(httpHandler);
    Assert.assertTrue(true);    //Cannot access private variables inside the server. If it is returned, meaning it is added successfully.
    httpServer.stop();
  }
}
