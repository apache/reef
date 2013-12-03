/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.tests.files;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.tests.TestEnvironment;
import com.microsoft.reef.tests.TestEnvironmentFactory;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationModule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tests whether a set of files makes it to the Driver and from there to the Evaluator.
 * <p/>
 * The test is shallow: It only makes sure that files with the same (random) names exist. It doesn't check for file
 * contents.
 */
public class FileResourceTest {
  private static final Logger LOG = Logger.getLogger(FileResourceTest.class.getName());
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();
  /**
   * The number of files to generate.
   */
  private final int nFiles = 3;

  /**
   * Assembles the driver configuration using the DriverConfiguration class.
   *
   * @param theFiles
   * @return
   * @throws BindException
   */
  private static Configuration getDriverConfiguration(final Set<File> theFiles) throws BindException {
    ConfigurationModule driverConfigurationModule = EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "FileResourceTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, Driver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, Driver.EvaluatorAllocatedHandler.class);

    for (final File f : theFiles) {
      LOG.log(Level.INFO, "Adding a file to the DriverConfiguration: " + f.getAbsolutePath());
      driverConfigurationModule = driverConfigurationModule.set(DriverConfiguration.LOCAL_FILES, f.getAbsolutePath());
    }
    return driverConfigurationModule.build();
  }

  /**
   * Assembles the configuration based on TestDriverConfiguration
   *
   * @param theFiles
   * @return
   * @throws BindException
   * @throws IOException
   */
  private static Configuration getTestDriverConfiguration(final Set<File> theFiles) throws BindException, IOException {
    ConfigurationModule testDriverConfigurationModule = TestDriverConfiguration.CONF;
    for (final File f : theFiles) {
      LOG.log(Level.INFO, "Adding a file to the TestDriverConfiguration: " + f.getName());
      testDriverConfigurationModule = testDriverConfigurationModule.set(TestDriverConfiguration.EXPECTED_FILE_NAME, f.getName());
    }

    final Configuration testDriverConfiguration = testDriverConfigurationModule.build();
    return testDriverConfiguration;
  }

  /**
   * Creates the given number of temp files.
   *
   * @param n
   * @return
   * @throws IOException
   */
  private static Set<File> getTempFiles(final int n) throws IOException {
    final Set<File> theFiles = new HashSet<>();
    for (int i = 0; i < n; ++i) {
      final File tempFile = File.createTempFile("REEF_TEST_", ".tmp");
      tempFile.deleteOnExit();
      theFiles.add(tempFile);
    }
    return theFiles;
  }

  /**
   * Merges the given Configurations.
   *
   * @param configurations
   * @return
   * @throws BindException
   */
  static Configuration merge(final Configuration... configurations) throws BindException {
    return Tang.Factory.getTang().newConfigurationBuilder(configurations).build();
  }

  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  @Test
  public void testDriverFiles() throws IOException, BindException, InjectionException {

    final Set<File> theFiles = getTempFiles(this.nFiles);
    final Configuration finalDriverConfiguration = merge(
        getDriverConfiguration(theFiles), getTestDriverConfiguration(theFiles));

    final LauncherStatus status = DriverLauncher
        .getLauncher(this.testEnvironment.getRuntimeConfiguration())
        .run(finalDriverConfiguration, testEnvironment.getTestTimeout());

    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }
}
