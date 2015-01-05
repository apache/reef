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
package org.apache.reef.webserver;

import org.apache.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import org.apache.reef.runtime.common.launch.REEFMessageCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Http Server
 */
public class TestHttpServer {

  /**
   * This is to test the case when using HttpRuntimeConfiguration.CONF with all the ther default bindings
   * @throws Exception
   */
  @Test
  public void httpServerDefaultCONFTest() throws Exception {
    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();
    final Injector injector = Tang.Factory.getTang().newInjector(httpRuntimeConfiguration);
    final HttpServer httpServer = injector.getInstance(HttpServer.class);
    Assert.assertTrue(httpServer instanceof HttpServerImpl);
    httpServer.stop();
  }

  @Test
  public void httpServerSpecifiedPortTest() throws Exception {

    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();

    final Configuration httpServerConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(PortNumber.class, "9000")
        .build();

    final Configuration configuration =
        Configurations.merge(httpRuntimeConfiguration, httpServerConfiguration);

    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final HttpServer httpServer = injector.getInstance(HttpServer.class);
    Assert.assertNotNull(httpServer);
    httpServer.stop();
  }

  @Test
  public void httpServerConflictPortTest() throws Exception {

    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();

    final Injector injector1 = Tang.Factory.getTang().newInjector(httpRuntimeConfiguration);
    final HttpServer httpServer1 = injector1.getInstance(HttpServer.class);

    final Injector injector2 = Tang.Factory.getTang().newInjector(httpRuntimeConfiguration);
    final HttpServer httpServer2 = injector2.getInstance(HttpServer.class);

    Assert.assertNotEquals(8080, httpServer2.getPort());

    httpServer1.stop();
    httpServer2.stop();
  }

  @Test
  public void httpServerPortRangeTest() throws Exception {
    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();

    final Configuration httpServerConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(PortNumber.class, "6000")
        .bindNamedParameter(MaxPortNumber.class, "9900")
        .bindNamedParameter(MinPortNumber.class, "1000")
        .bindNamedParameter(MaxRetryAttempts.class, "3")
        .build();

    final Configuration configuration =
        Configurations.merge(httpRuntimeConfiguration, httpServerConfiguration);

    final Injector injector1 = Tang.Factory.getTang().newInjector(configuration);
    final HttpServer httpServer1 = injector1.getInstance(HttpServer.class);

    final Injector injector2 = Tang.Factory.getTang().newInjector(configuration);
    final HttpServer httpServer2 = injector2.getInstance(HttpServer.class);

    Assert.assertTrue("port number is out of specified range",
        httpServer2.getPort() >= 1000 && httpServer2.getPort() <= 9900);

    httpServer1.stop();
    httpServer2.stop();
  }

  @Test
  public void httpServerPortRetryTest() throws Exception {

    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();
    final Injector injector1 = Tang.Factory.getTang().newInjector(httpRuntimeConfiguration);
    final HttpServer httpServer1 = injector1.getInstance(HttpServer.class);
    final String portUsed = "" + httpServer1.getPort();

    final Configuration httpServerConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(PortNumber.class, portUsed)
        .bindNamedParameter(MaxPortNumber.class, portUsed)
        .bindNamedParameter(MinPortNumber.class, portUsed)
        .bindNamedParameter(MaxRetryAttempts.class, "3")
        .build();

    final Configuration configuration =
        Configurations.merge(httpRuntimeConfiguration, httpServerConfiguration);

    final Injector injector2 = Tang.Factory.getTang().newInjector(configuration);

    try {
      injector2.getInstance(HttpServer.class);
      Assert.fail("Created two web servers on the same port: " + portUsed);
    } catch (final InjectionException ex) {
      Assert.assertEquals("Could not find available port in 3 attempts", ex.getCause().getMessage());
    }

    httpServer1.stop();
  }

  @Test
  public void httpServerAddHandlerTest() throws Exception {

    final Configuration httpRuntimeConfiguration = HttpRuntimeConfiguration.CONF.build();

    final Configuration reefConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_TEST_REMOTE_MANAGER")
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, "my job")
        .build();

    final Configuration finalConfig =
        Configurations.merge(httpRuntimeConfiguration, reefConfiguration);

    final Injector injector = Tang.Factory.getTang().newInjector(finalConfig);
    final HttpServer httpServer = injector.getInstance(HttpServer.class);
    final HttpServerReefEventHandler httpHandler =
        injector.getInstance(HttpServerReefEventHandler.class);

    httpServer.addHttpHandler(httpHandler);

    // Assert.assertTrue(true);
    // Cannot access private variables inside the server.
    // If it is returned, meaning it is added successfully.

    httpServer.stop();
  }
}
