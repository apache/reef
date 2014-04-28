package com.microsoft.reef.examples.hello;

import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.webserver.HttpHandlerConfiguration;
import com.microsoft.reef.webserver.HttpServerReefEventHandler;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

/**
 * Example to run HelloREEF with a webserver.
 */
public final class HelloREEFHttp {

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
    final LauncherStatus status = runHelloReef(runtimeConfiguration, HelloREEF.JOB_TIMEOUT);

  }
}
