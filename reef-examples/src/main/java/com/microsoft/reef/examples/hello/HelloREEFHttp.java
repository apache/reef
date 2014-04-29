package com.microsoft.reef.examples.hello;

import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManager;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.webserver.*;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Example to run HelloREEF with a webserver.
 */
public final class HelloREEFHttp {
    private static final Logger LOG = Logger.getLogger(HelloREEFHttp.class.getName());

    /**
     * Number of milliseconds to wait for the job to complete.
     */
    public static final int JOB_TIMEOUT = 1000000; // 1000 sec.


  /**
   * @return the driver-side configuration to be merged into the DriverConfiguration to enable the HTTP server.
   */
  public static Configuration getHTTPConfiguration() {
    return HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerReefEventHandler.class)
        .build();
  }

  public static LauncherStatus runHelloReef(final Configuration runtimeConf, final int timeOut)
      throws BindException, InjectionException {
    final Configuration driverConf = Configurations.merge(HelloREEF.getDriverConfiguration(), getHTTPConfiguration());
    return DriverLauncher.getLauncher(runtimeConf).run(driverConf, timeOut);
  }

  public static void main(final String[] args) throws InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 2)
        .build();
    final LauncherStatus status = runHelloReef(runtimeConfiguration, HelloREEFHttp.JOB_TIMEOUT);

  }
}

/**
 * HttpServerReefEventHandler
 */
//final class HttpServerReefEventHandler implements HttpHandler {
//    private static final Logger LOG = Logger.getLogger(HttpServerReefEventHandler.class.getName());
//    /**
//     *  HttpServerReefEventHandler constructor.
//     */
//    @Inject
//    public HttpServerReefEventHandler(EvaluatorManager evaluatorManager ) {
//        LOG.log(Level.INFO, "HttpServerReefEventHandler is instantiated. evaluatorManager: " + evaluatorManager.toString());
//    }
//
//    @Inject
//    public HttpServerReefEventHandler() {
//        LOG.log(Level.INFO, "HttpServerReefEventHandler is instantiated.");
//    }
//
//    /**
//     * returns URI specification for the handler
//     * @return
//     */
//    @Override
//    public String getUriSpecification() {
//        return "/Reef/";
//    }
//
//    /**
//     * it is called when receiving a http request
//     * @param request
//     * @param response
//     */
//    @Override
//    public void onHttpRequest(HttpRequest request, HttpResponse response) {
//        //TODO
//        LOG.log(Level.INFO, "HttpServerReefEventHandler onHttpRequest is called");
//    }
//}
