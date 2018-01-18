package org.apache.reef.examples.hello;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runime.azbatch.client.AzureBatchRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.ThreadLogger;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A main() for running hello REEF in Azure Batch.
 */
public class HelloReefAzBatch {
  private static final Logger LOG = Logger.getLogger(HelloReefAzBatch.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 60000; // 60 sec.

  /**
   * Builds the rutime configuration for Azure Batch.
   *
   * @return the configuration of the runtime.
   * @throws IOException
   */
  private static Configuration getRuntimeConfiguration() throws IOException {
    return AzureBatchRuntimeConfiguration.fromEnvironment();
  }

  /**
   * Builds and returns driver configuration for HelloREEF driver.
   *
   * @return the configuration of the HelloREEF driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "HelloREEF")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(HelloDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, HelloDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HelloDriver.EvaluatorAllocatedHandler.class)
        .build();
  }

  /**
   * Start Hello REEF job with AzBatch runtime.
   *
   * @param args command line parameters.
   * @throws InjectionException configuration error.
   * @throws IOException
   */
  public static void main(final String[] args) throws InjectionException, IOException {

    Configuration runtimeConfiguration = getRuntimeConfiguration();
    Configuration driverConfiguration = getDriverConfiguration();

    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, JOB_TIMEOUT);

    LOG.log(Level.INFO, "REEF job completed: {0}", status);
    ThreadLogger.logThreads(LOG, Level.FINE, "Threads running at the end of HelloREEF:");
  }
}
