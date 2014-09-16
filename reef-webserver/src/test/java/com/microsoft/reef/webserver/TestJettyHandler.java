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
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.remote.RemoteConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.io.bio.StringEndPoint;
import org.mortbay.jetty.*;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Unit Test for Jetty Handler
 */
public class TestJettyHandler {

  private Request request;
  private Response response;
  private JettyHandler handler;

  @Before
  public void setUp() throws InjectionException, IOException, ServletException {

    this.request = new Request(
        new HttpConnection(new LocalConnector(), new StringEndPoint(), new Server()));

    this.request.setContentType("text/json");

    this.response = new Response(
        new HttpConnection(new LocalConnector(), new StringEndPoint(), new Server()));

    final Configuration httpHandlerConfiguration = HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();

    final Tang tang = Tang.Factory.getTang();

    final Configuration remoteConfiguration = tang.newConfigurationBuilder()
        .bindNamedParameter(RemoteConfiguration.ManagerName.class, "REEF_TEST_REMOTE_MANAGER")
        .bindNamedParameter(RemoteConfiguration.MessageCodec.class, REEFMessageCodec.class)
        .bindNamedParameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class, "my job")
        .build();

    final Configuration finalConfig =
        Configurations.merge(httpHandlerConfiguration, remoteConfiguration);

    final Injector injector = tang.newInjector(finalConfig);

    this.handler = injector.getInstance(JettyHandler.class);
  }

  @Test
  public void testWithoutQueryString() throws IOException, ServletException {
    this.request.setUri(new HttpURI("http://microsoft.com:8080/Reef/v1/Evaluators/"));
    this.handler.handle("target", this.request, this.response, 0);
    Assert.assertEquals(HttpServletResponse.SC_OK, this.response.getStatus());
  }

  @Test
  public void testWithoutSpecification() throws IOException, ServletException {
    this.request.setUri(new HttpURI("http://microsoft.com:8080/"));
    this.handler.handle("target", this.request, this.response, 0);
    Assert.assertEquals(HttpServletResponse.SC_BAD_REQUEST, this.response.getStatus());
  }

  @Test
  public void testWithoutVersion() throws IOException, ServletException {
    this.request.setUri(new HttpURI("http://microsoft.com:8080/reef"));
    this.handler.handle("target", this.request, this.response, 0);
    Assert.assertEquals(HttpServletResponse.SC_BAD_REQUEST, this.response.getStatus());
  }

  @Test
  public void testWithQueryString() throws IOException, ServletException {
    this.request.setUri(new HttpURI("http://microsoft.com:8080/Reef/v1/Evaluators"));
    this.request.setQueryString("id=12345");
    this.handler.handle("target", this.request, this.response, 0);
    Assert.assertEquals(HttpServletResponse.SC_OK, this.response.getStatus());
  }

  @Test
  public void testWithUnSupportedSpec() throws IOException, ServletException {
    this.request.setUri(new HttpURI("http://microsoft.com:8080/abc/v2"));
    this.request.setQueryString("id=12345");
    this.handler.handle("target", this.request, this.response, 0);
    Assert.assertEquals(HttpServletResponse.SC_NOT_FOUND, this.response.getStatus());
  }

  @Test
  public void testAddHandler() throws InjectionException, IOException, ServletException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final HttpAbcEventHandler abcEventHandler = injector.getInstance(HttpAbcEventHandler.class);
    this.handler.addHandler(abcEventHandler);
    this.request.setUri(new HttpURI("http://microsoft.com:8080/Abc/v1"));
    this.handler.handle("target", this.request, this.response, 0);
    Assert.assertEquals(HttpServletResponse.SC_OK, this.response.getStatus());
  }

  @Test
  public void testAddDuplicatedHandler() throws InjectionException, IOException, ServletException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final HttpAbcEventHandler abcEventHandler = injector.getInstance(HttpAbcEventHandler.class);
    this.handler.addHandler(abcEventHandler);
    this.handler.addHandler(abcEventHandler);  // it will be ignored
    this.request.setUri(new HttpURI("http://microsoft.com:8080/Abc/v1"));
    this.handler.handle("target", this.request, this.response, 0);
    Assert.assertEquals(HttpServletResponse.SC_OK, this.response.getStatus());
  }

  @Test
  public void testAddNullHandler() {
    this.handler.addHandler(null); //nothing will be added
    Assert.assertTrue(true);
  }
}

final class HttpAbcEventHandler implements HttpHandler {

  @Inject
  public HttpAbcEventHandler() {
  }

  @Override
  public String getUriSpecification() {
    return "Abc";
  }

  @Override
  public void setUriSpecification(final String s) {
  }

  @Override
  public void onHttpRequest(
      final ParsedHttpRequest parsedHttpRequest,
      final HttpServletResponse response) throws IOException, ServletException {
    response.getWriter().println("OnRequest in HttpAbcEventHandler is called");
  }
}
