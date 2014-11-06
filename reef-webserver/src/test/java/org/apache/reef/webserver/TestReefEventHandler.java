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
import org.junit.Before;
import org.junit.Test;
import org.mortbay.io.bio.StringEndPoint;
import org.mortbay.jetty.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Test Http Server Reef Event Handler
 */
public class TestReefEventHandler {
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
  public void testGetEvaluatorList() throws IOException, ServletException {
    this.request.setUri(new HttpURI("http://microsoft.com:8080/Reef/v1/Evaluators/"));
    this.handler.handle("target", this.request, this.response, 0);
    Assert.assertEquals(HttpServletResponse.SC_OK, this.response.getStatus());
  }
}
