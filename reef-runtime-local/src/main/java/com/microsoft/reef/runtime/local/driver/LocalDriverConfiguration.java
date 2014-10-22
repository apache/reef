/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.runtime.local.driver;

import com.microsoft.reef.runtime.common.files.RuntimeClasspathProvider;
import com.microsoft.reef.runtime.common.parameters.JVMHeapSlack;
import com.microsoft.reef.runtime.local.LocalClasspathProvider;
import com.microsoft.reef.runtime.local.client.parameters.NumberOfProcesses;
import com.microsoft.reef.runtime.local.client.parameters.RootFolder;
import com.microsoft.reef.runtime.local.driver.parameters.GlobalFiles;
import com.microsoft.reef.runtime.local.driver.parameters.GlobalLibraries;
import com.microsoft.reef.runtime.local.driver.parameters.LocalFiles;
import com.microsoft.reef.runtime.local.driver.parameters.LocalLibraries;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.RequiredParameter;

/**
 * ConfigurationModule for the Driver executed in the local resourcemanager. This is meant to eventually replace
 * LocalDriverRuntimeConfiguration.
 */
public class LocalDriverConfiguration extends ConfigurationModuleBuilder {

  /**
   * Files for the driver only.
   */
  public static final OptionalParameter<String> LOCAL_FILES = new OptionalParameter<>();
  /**
   * Libraries for the driver only.
   */
  public static final OptionalParameter<String> LOCAL_LIBRARIES = new OptionalParameter<>();
  /**
   * Files for the driver and all evaluators.
   */
  public static final OptionalParameter<String> GLOBAL_FILES = new OptionalParameter<>();
  /**
   * Libraries for the driver and all evaluators.
   */
  public static final OptionalParameter<String> GLOBAL_LIBRARIES = new OptionalParameter<>();
  /**
   * The maximum number or processes to spawn.
   */
  public static final RequiredParameter<Integer> NUMBER_OF_PROCESSES = new RequiredParameter<>();
  /**
   * The root folder of the job. Assumed to be an absolute path.
   */
  public static final RequiredParameter<String> ROOT_FOLDER = new RequiredParameter<>();
  /**
   * The fraction of the container memory NOT to use for the Java Heap.
   */
  public static final OptionalParameter<Double> JVM_HEAP_SLACK = new OptionalParameter<>();


  public static final ConfigurationModule CONF = new LocalDriverConfiguration()
      .bindSetEntry(LocalFiles.class, LOCAL_FILES)
      .bindSetEntry(LocalLibraries.class, LOCAL_LIBRARIES)
      .bindSetEntry(GlobalFiles.class, GLOBAL_FILES)
      .bindSetEntry(GlobalLibraries.class, GLOBAL_LIBRARIES)
      .bindNamedParameter(NumberOfProcesses.class, NUMBER_OF_PROCESSES)
      .bindNamedParameter(RootFolder.class, ROOT_FOLDER)
      .bindNamedParameter(JVMHeapSlack.class, JVM_HEAP_SLACK)
      .bindImplementation(RuntimeClasspathProvider.class, LocalClasspathProvider.class)
      .build();
}
