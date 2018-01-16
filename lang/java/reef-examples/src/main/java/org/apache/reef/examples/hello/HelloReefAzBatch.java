package org.apache.reef.examples.hello;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runime.azbatch.AzureBatchRuntimeConfiguration;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.ThreadLogger;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A main() for running hello REEF in Azure Batch.
 */
public class HelloReefAzBatch {
    private static final Logger LOG = Logger.getLogger(HelloReefAzBatch.class.getName());

    /** Number of milliseconds to wait for the job to complete. */
    private static final int JOB_TIMEOUT = 60000; // 60 sec.

    /**
     * Builds the rutime configuration for Azure Batch.
     * @return the configuration of the runtime.
     */
    private static Configuration getRuntimeConfiguration() {
        return AzureBatchRuntimeConfiguration.CONF
                .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_URI, "")
                .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_NAME, "")
                .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_ACCOUNT_KEY, "")
                .set(AzureBatchRuntimeConfiguration.AZURE_BATCH_POOL_ID, "")
                .build();
    }

    /**
     * Builds and returns driver configuration for HelloREEF driver.
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
     * @param args command line parameters.
     * @throws InjectionException configuration error.
     */
    public static void main(final String[] args) throws InjectionException {

        Configuration runtimeConfiguration = getRuntimeConfiguration();
        Configuration driverConfiguration = getDriverConfiguration();

        final LauncherStatus status = DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, JOB_TIMEOUT);

        LOG.log(Level.INFO, "REEF job completed: {0}", status);
        ThreadLogger.logThreads(LOG, Level.FINE, "Threads running at the end of HelloREEF:");
    }
}
