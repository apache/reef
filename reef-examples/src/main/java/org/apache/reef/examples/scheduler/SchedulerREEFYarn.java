package org.apache.reef.examples.scheduler;

import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;

import static org.apache.reef.examples.scheduler.SchedulerREEF.runTaskScheduler;

/**
 * REEF TaskScheduler on YARN runtime.
 */
public final class SchedulerREEFYarn {
  /**
   * Launch the scheduler with YARN client configuration
   * @param args
   * @throws InjectionException
   * @throws java.io.IOException
   */
  public final static void main(String[] args) throws InjectionException, IOException {
    final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();
    runTaskScheduler(runtimeConfiguration, args);
  }
}
