package com.microsoft.reef.webserver;

import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.launch.REEFMessageCodec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.remote.RemoteConfiguration;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.io.bio.StringEndPoint;
import org.mortbay.jetty.*;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

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
    ServletOutputStream outputStream = response.getOutputStream();
    Assert.assertEquals(HttpServletResponse.SC_OK, this.response.getStatus());
  }
}
