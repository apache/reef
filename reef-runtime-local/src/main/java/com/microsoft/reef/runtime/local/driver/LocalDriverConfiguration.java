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

import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.RequiredParameter;

import java.util.Set;

/**
 * ConfigurationModule for the Driver executed in the local resourcemanager. This is meant to eventually replace
 * LocalDriverRuntimeConfiguration.
 */
public class LocalDriverConfiguration extends ConfigurationModuleBuilder {

  @NamedParameter(doc = "The names of files that are to be kept on the driver only.")
  public static final class LocalFiles implements Name<Set<String>> {
  }

  @NamedParameter(doc = "The names of files that are to be copied to all evaluators.")
  public static final class GlobalFiles implements Name<Set<String>> {
  }

  @NamedParameter(doc = "The names of files that are to be kept on the driver only.")
  public static final class LocalLibraries implements Name<Set<String>> {
  }

  @NamedParameter(doc = "The names of files that are to be copied to all evaluators.")
  public static final class GlobalLibraries implements Name<Set<String>> {
  }

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


  public static final ConfigurationModule CONF = new LocalDriverConfiguration()
      .bindSetEntry(LocalFiles.class, LOCAL_FILES)
      .bindSetEntry(LocalLibraries.class, LOCAL_LIBRARIES)
      .bindSetEntry(GlobalFiles.class, GLOBAL_FILES)
      .bindSetEntry(GlobalLibraries.class, GLOBAL_LIBRARIES)
      .bindNamedParameter(LocalRuntimeConfiguration.NumberOfThreads.class, NUMBER_OF_PROCESSES)
      .bindNamedParameter(LocalRuntimeConfiguration.RootFolder.class, ROOT_FOLDER)
      .build();
}
