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
package org.apache.reef.io;

import org.apache.reef.annotations.Provided;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.FileAttribute;

/**
 * Utility to create temporary files and folders in accordance with the underlying resource manager.
 */
@Provided
@DefaultImplementation(WorkingDirectoryTempFileCreator.class)
public interface TempFileCreator {
  /**
   * Creates a temporary file.
   *
   * @param prefix the prefix for the file
   * @param suffix the suffix for the file
   * @return the created file.
   * @throws IOException
   */
  File createTempFile(String prefix, String suffix) throws IOException;

  /**
   * Creates a temporary folder.
   *
   * @param prefix the prefix for the file
   * @param attrs the attributes for the file
   * @return the created folder.
   * @throws IOException
   */
  File createTempDirectory(String prefix, FileAttribute<?> attrs) throws IOException;


  /**
   * Create a temporary folder.
   *
   * @param prefix the prefix for the folder
   * @return the created folder
   * @throws IOException
   */
  File createTempDirectory(String prefix) throws IOException;


}
