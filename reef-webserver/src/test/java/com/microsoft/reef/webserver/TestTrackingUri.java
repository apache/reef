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

import com.microsoft.reef.runtime.yarn.driver.TrackingURLProvider;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;

/**
 * Tracking Uri test
 */
public class TestTrackingUri {
  /**
   * Get Default Tracking URI
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
   * Get Tracking URI with specified port number and HttpTrackingURLProvider
   *
   * @throws InjectionException
   * @throws UnknownHostException
   * @throws BindException
   */
  @Test
  public void testHttpTrackingUri() throws InjectionException, UnknownHostException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(PortNumber.class, "8888")
        .bindImplementation(TrackingURLProvider.class, HttpTrackingURLProvider.class)
        .bindImplementation(HttpServer.class, HttpServerImpl.class);

    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final String uri = injector.getInstance(TrackingURLProvider.class).getTrackingUrl();
    final int port = injector.getInstance(HttpServer.class).getPort();
    verifyUri(uri, port);
  }

  /**
   * Get Tracking URI with HttpTrackingURLProvider and defualt port number
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
   * Http Tracking URI using default binding test
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
   * Verify if URI is correct
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
