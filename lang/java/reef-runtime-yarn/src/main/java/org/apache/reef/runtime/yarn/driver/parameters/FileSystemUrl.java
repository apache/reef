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
package org.apache.reef.runtime.yarn.driver.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * The file system URL.
 * For Hadoop file system, it is set in fs.defaultFS as default by YARN environment.
 * For Data Lake, for example, Yarn applications are required to set the complete path by themselves.
 * Example is adl://reefadl.azuredatalakestore.net.
 */
@NamedParameter(doc = "The File System URL.", default_value = "NULL")
public final class FileSystemUrl implements Name<String> {

  private FileSystemUrl() {
  }

  public static final String DEFAULT_VALUE = "NULL";
}
