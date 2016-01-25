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
package org.apache.reef.tests.files;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

import java.util.Set;

/**
 * A ConfigurationModule for the TestTask.
 */
public final class FileResourceTestTaskConfiguration extends ConfigurationModuleBuilder {
  /**
   * The set of file names to expect present on the evaluator.
   */
  public static final RequiredParameter<String> EXPECTED_FILE_NAME = new RequiredParameter<>();
  public static final ConfigurationModule CONF = new FileResourceTestTaskConfiguration()
      .bindSetEntry(FileNamesToExpect.class, EXPECTED_FILE_NAME)
      .build();

  /**
   * The names of the files to expect in the local filesystem.
   */
  @NamedParameter(doc = "The names of the files to expect in the local filesystem.")
  public static final class FileNamesToExpect implements Name<Set<String>> {
  }
}
