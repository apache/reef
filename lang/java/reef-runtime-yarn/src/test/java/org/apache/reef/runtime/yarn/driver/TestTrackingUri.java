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
package org.apache.reef.runtime.yarn.driver;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.ports.parameters.TcpPortRangeBegin;
import org.apache.reef.webserver.HttpHandlerConfiguration;
import org.apache.reef.webserver.HttpServer;
import org.apache.reef.webserver.HttpServerImpl;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;

/**
 * Tracking Uri test.
 */
public class TestTrackingUri {
  /**
   * Get Default Tracking URI.
   *
   * @throws InjectionException
   * @throws UnknownHostException
   */
  @Test
  public void testDefaultTrackingUri() throws InjectionException, UnknownHostException {
    final String uri = Tang.Factory.getTang().newInjector().getInstance(TrackingURLProvider.class).getTrackingUrl();
    Assert.assertEquals(uri, "");
  }

  /**
   * Get Tracking URI with specified port number and HttpTrackingURLProvider.
   *
   * @throws InjectionException
   * @throws UnknownHostException
   * @throws BindException
   */
  @Test
  public void testHttpTrackingUri() throws InjectionException, UnknownHostException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(TcpPortRangeBegin.class, "8888")
        .bindImplementation(TrackingURLProvider.class, HttpTrackingURLProvider.class)
        .bindImplementation(HttpServer.class, HttpServerImpl.class);

    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final String uri = injector.getInstance(TrackingURLProvider.class).getTrackingUrl();
    final int port = injector.getInstance(HttpServer.class).getPort();
    verifyUri(uri, port);
  }

  /**
   * Get Tracking URI with HttpTrackingURLProvider and defualt port number.
   *
   * @throws InjectionException
   * @throws UnknownHostException
   * @throws BindException
   */
  @Test
  public void testHttpTrackingUriDefaultPort() throws InjectionException, UnknownHostException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(HttpServer.class, HttpServerImpl.class)
        .bindImplementation(TrackingURLProvider.class, HttpTrackingURLProvider.class);

    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final String uri = injector.getInstance(TrackingURLProvider.class).getTrackingUrl();
    final int port = injector.getInstance(HttpServer.class).getPort();
    verifyUri(uri, port);
  }

  /**
   * Http Tracking URI using default binding test.
   *
   * @throws InjectionException
   * @throws UnknownHostException
   * @throws BindException
   */
  @Test
  public void testHttpTrackingUriDefaultBinding() throws InjectionException, UnknownHostException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(HttpHandlerConfiguration.CONF.build());
    final String uri = injector.getInstance(TrackingURLProvider.class).getTrackingUrl();
    final int port = injector.getInstance(HttpServer.class).getPort();
    verifyUri(uri, port);
  }

  /**
   * Verify if URI is correct.
   *
   * @param uri
   * @param port
   */
  private void verifyUri(final String uri, final int port) {
    final String[] parts = uri.split(":");
    Assert.assertTrue(parts.length == 2);
    Assert.assertEquals(port, Integer.parseInt(parts[1]));
  }
}
