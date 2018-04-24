/*
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
package org.apache.reef.runtime.azbatch;

import org.apache.reef.runtime.azbatch.util.command.LinuxCommandBuilder;
import org.apache.reef.runtime.azbatch.util.command.WindowsCommandBuilder;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the CommandBuilder functions.
 */
public final class CommandBuilderTests {

  private Injector injector;
  private LinuxCommandBuilder linuxCommandBuilder;
  private WindowsCommandBuilder windowsCommandBuilder;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws InjectionException {
    this.injector = Tang.Factory.getTang().newInjector();
    RuntimeClasspathProvider classpathProvider = mock(RuntimeClasspathProvider.class);
    when(classpathProvider.getDriverClasspathPrefix()).thenReturn(Arrays.asList("c:\\driverpath1", "c:\\driverpath2"));
    when(classpathProvider.getEvaluatorClasspathPrefix())
        .thenReturn(Arrays.asList("c:\\evaluatorpath1", "c:\\evaluatorpath2"));
    when(classpathProvider.getDriverClasspathSuffix()).thenReturn(Arrays.asList("driverclasspathsuffix"));
    when(classpathProvider.getEvaluatorClasspathSuffix()).thenReturn(Arrays.asList("evaluatorclasspathsuffix"));
    this.injector
        .bindVolatileInstance(RuntimeClasspathProvider.class, classpathProvider);
    this.linuxCommandBuilder = this.injector.getInstance(LinuxCommandBuilder.class);
    this.windowsCommandBuilder = this.injector.getInstance(WindowsCommandBuilder.class);

  }

  @Test
  public void linuxCommandBuilderDriverTest() {
    JobSubmissionEvent event = mock(JobSubmissionEvent.class);

    Optional<Integer> memory = Optional.of(100);
    when(event.getDriverMemory()).thenReturn(memory);

    String actual = this.linuxCommandBuilder.buildDriverCommand(event);
    String expected =
        "/bin/sh -c \"unzip local.jar -d 'reef/'; {{JAVA_HOME}}/bin/java -Xmx100m -XX:PermSize=128m " +
            "-XX:MaxPermSize=128m -ea -classpath " +
            "c:\\driverpath1:c:\\driverpath2:reef/local/*:reef/global/*:driverclasspathsuffix " +
            "-Dproc_reef org.apache.reef.runtime.common.REEFLauncher reef/local/driver.conf\"";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void windowsCommandBuilderDriverTest() {
    JobSubmissionEvent event = mock(JobSubmissionEvent.class);

    Optional<Integer> memory = Optional.of(100);
    when(event.getDriverMemory()).thenReturn(memory);

    String actual = this.windowsCommandBuilder.buildDriverCommand(event);
    String expected = "powershell.exe /c \"Add-Type -AssemblyName System.IO.Compression.FileSystem;  " +
        "[System.IO.Compression.ZipFile]::ExtractToDirectory(\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\local.jar\\\", " +
        "\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\reef\\\");  {{JAVA_HOME}}/bin/java -Xmx100m -XX:PermSize=128m " +
        "-XX:MaxPermSize=128m -ea -classpath " +
        "'c:\\driverpath1;c:\\driverpath2;reef/local/*;reef/global/*;driverclasspathsuffix' " +
        "-Dproc_reef org.apache.reef.runtime.common.REEFLauncher reef/local/driver.conf\";";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void linuxCommandBuilderShimEvaluatorTest() {
    String actual = this.linuxCommandBuilder.buildEvaluatorShimCommand(1, "conf");
    String expected = "/bin/sh -c \"unzip local.jar -d 'reef/'; {{JAVA_HOME}}/bin/java -Xmx1m " +
        "-XX:PermSize=128m -XX:MaxPermSize=128m -ea " +
        "-classpath c:\\evaluatorpath1:c:\\evaluatorpath2:reef/local/*:reef/global/*:evaluatorclasspathsuffix " +
        "-Dproc_reef org.apache.reef.runtime.azbatch.evaluator.EvaluatorShimLauncher conf\"";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void windowsCommandBuilderShimEvaluatorTest() {
    String actual = this.windowsCommandBuilder.buildEvaluatorShimCommand(1, "conf");
    String expected = "powershell.exe /c \"Add-Type -AssemblyName System.IO.Compression.FileSystem;  " +
        "[System.IO.Compression.ZipFile]::ExtractToDirectory(\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\local.jar\\\", " +
        "\\\"$env:AZ_BATCH_TASK_WORKING_DIR\\reef\\\");  {{JAVA_HOME}}/bin/java -Xmx1m -XX:PermSize=128m " +
        "-XX:MaxPermSize=128m -ea -classpath " +
        "'c:\\evaluatorpath1;c:\\evaluatorpath2;reef/local/*;reef/global/*;evaluatorclasspathsuffix' -Dproc_reef " +
        "org.apache.reef.runtime.azbatch.evaluator.EvaluatorShimLauncher conf\";";
    Assert.assertEquals(expected, actual);
  }
}
