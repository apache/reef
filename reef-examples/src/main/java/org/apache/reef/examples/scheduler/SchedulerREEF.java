package org.apache.reef.examples.scheduler;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverServiceConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.webserver.HttpHandlerConfiguration;
import org.apache.reef.webserver.ReefEventStateManager;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;

/**
 * REEF TaskScheduler.
 */
public final class SchedulerREEF {

  @NamedParameter(doc="The command of this job", short_name="cmd")
  public static final class Command implements Name<String> {
  }

  /**
   * Command line parameter = true to reuse evaluators, or false allocate/close for each iteration
   */
  @NamedParameter(doc = "Whether or not to reuse evaluators",
    short_name = "retain", default_value = "true")
  public static final class Retain implements Name<Boolean> {
  }

  /**
   * @return The http configuration to use reef-webserver
   */
  public final static Configuration getHttpConf() {
    final Configuration httpHandlerConf = HttpHandlerConfiguration.CONF
      .set(HttpHandlerConfiguration.HTTP_HANDLERS, HttpServerShellCmdHandler.class)
      .build();

    final Configuration httpServiceConf = DriverServiceConfiguration.CONF
      .set(DriverServiceConfiguration.ON_EVALUATOR_ALLOCATED, ReefEventStateManager.AllocatedEvaluatorStateHandler.class)
      .set(DriverServiceConfiguration.ON_CONTEXT_ACTIVE, ReefEventStateManager.ActiveContextStateHandler.class)
      .set(DriverServiceConfiguration.ON_TASK_RUNNING, ReefEventStateManager.TaskRunningStateHandler.class)
      .set(DriverServiceConfiguration.ON_DRIVER_STARTED, ReefEventStateManager.StartStateHandler.class)
      .set(DriverServiceConfiguration.ON_DRIVER_STOP, ReefEventStateManager.StopStateHandler.class)
      .build();
    return Configurations.merge(httpHandlerConf, httpServiceConf);

  }

  /**
   * @return The Driver configuration.
   */
  public final static Configuration getDriverConf() {
    final Configuration driverConf = DriverConfiguration.CONF
      .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(SchedulerDriver.class))
      .set(DriverConfiguration.DRIVER_IDENTIFIER, "TaskScheduler")
      .set(DriverConfiguration.ON_DRIVER_STARTED, SchedulerDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SchedulerDriver.EvaluatorAllocatedHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_ACTIVE, SchedulerDriver.ActiveContextHandler.class)
      .set(DriverConfiguration.ON_TASK_COMPLETED, SchedulerDriver.CompletedTaskHandler.class)
      .build();

    return driverConf;
  }

  /**
   * Run the Task scheduler. If '-retain true' option is passed via command line,
   * the scheduler reuses evaluators to submit new Tasks.
   * @param runtimeConf The runtime configuration (e.g. Local, YARN, etc)
   * @param args Command line arguments.
   * @throws InjectionException
   * @throws java.io.IOException
   */
  public static void runTaskScheduler(final Configuration runtimeConf, final String[] args)
    throws InjectionException, IOException {
    final Tang tang = Tang.Factory.getTang();

    // Process command line arguments to build configuration
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();
    new CommandLine(cb)
      .registerShortNameOfClass(Retain.class)
      .processCommandLine(args);
    final Configuration commandLineConf = cb.build();

    // Merge the configurations to run Driver
    final Configuration driverConf = Configurations.merge(getDriverConf(), getHttpConf(), commandLineConf);

    final REEF reef = tang.newInjector(runtimeConf).getInstance(REEF.class);
    reef.submit(driverConf);
  }

  /**
   * Main program
   * @param args
   * @throws InjectionException
   */
  public final static void main(String[] args) throws InjectionException, IOException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
      .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, 3)
      .build();
    runTaskScheduler(runtimeConfiguration, args);
  }
}
