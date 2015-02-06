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
package org.apache.reef.tests.files;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.TempFileCreator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.util.EnvironmentUtils;
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
public final class FileResourceTest {
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
    ConfigurationModule driverConfigurationModule = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(FileResourceTestDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_FileResourceTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, FileResourceTestDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, FileResourceTestDriver.EvaluatorAllocatedHandler.class);

    for (final File f : theFiles) {
      LOG.log(Level.FINEST, "Adding a file to the DriverConfiguration: " + f.getAbsolutePath());
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
    ConfigurationModule testDriverConfigurationModule = FileResourceTestDriverConfiguration.CONF;
    for (final File f : theFiles) {
      LOG.log(Level.FINEST, "Adding a file to the TestDriverConfiguration: " + f.getName());
      testDriverConfigurationModule = testDriverConfigurationModule.set(FileResourceTestDriverConfiguration.EXPECTED_FILE_NAME, f.getName());
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
  private Set<File> getTempFiles(final int n) throws IOException, InjectionException {
    final TempFileCreator tempFileCreator = Tang.Factory.getTang()
        .newInjector(testEnvironment.getRuntimeConfiguration())
        .getInstance(TempFileCreator.class);
    final Set<File> theFiles = new HashSet<>();
    for (int i = 0; i < n; ++i) {
      final File tempFile = tempFileCreator.createTempFile("REEF_TEST_", ".tmp");
      tempFile.deleteOnExit();
      theFiles.add(tempFile);
    }
    return theFiles;
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
    final Configuration finalDriverConfiguration = Configurations.merge(
        getDriverConfiguration(theFiles), getTestDriverConfiguration(theFiles));

    final LauncherStatus status = DriverLauncher
        .getLauncher(this.testEnvironment.getRuntimeConfiguration())
        .run(finalDriverConfiguration, testEnvironment.getTestTimeout());

    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
  }
}
