package com.microsoft.reef.tests.evaluatorsize;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.reef.util.TANGUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EvaluatorSizeTest {
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();

  @Before
  public void setUp() throws Exception {
    this.testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  private LauncherStatus runEvaluatorSizeTest(final int megaBytes) throws BindException, InjectionException {
    final Configuration runtimeConfiguration = this.testEnvironment.getRuntimeConfiguration();

    final Configuration testConfiguration = EvaluatorSizeTestConfiguration.CONF
        .set(EvaluatorSizeTestConfiguration.MEMORY_SIZE, 777)
        .build();

    final Configuration driverConfiguration =
        EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "EvaluatorSize-" + megaBytes)
            .set(DriverConfiguration.ON_DRIVER_STARTED, Driver.StartHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, Driver.EvaluatorAllocatedHandler.class).build();

    final Configuration mergedDriverConfiguration = TANGUtils.merge(driverConfiguration, testConfiguration);

    final LauncherStatus state = DriverLauncher.getLauncher(runtimeConfiguration)
        .run(mergedDriverConfiguration, this.testEnvironment.getTestTimeout());
    return state;
  }


  @Test
  public void testEvaluatorSize() throws BindException, InjectionException {
    final LauncherStatus state = runEvaluatorSizeTest(777);
    Assert.assertTrue("Job state after execution: " + state, state.isSuccess());
  }
}
