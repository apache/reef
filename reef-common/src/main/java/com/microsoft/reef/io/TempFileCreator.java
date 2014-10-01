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
package com.microsoft.reef.io;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.tang.annotations.DefaultImplementation;

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
   * @param prefix
   * @param suffix
   * @return
   * @throws IOException
   */
  public File createTempFile(final String prefix, final String suffix) throws IOException;

  /**
   * Creates a temporary folder.
   *
   * @param prefix
   * @param attrs
   * @return
   * @throws IOException
   */
  public File createTempDirectory(final String prefix, final FileAttribute<?> attrs) throws IOException;


  /**
   * Create a temporary folder.
   *
   * @param prefix
   * @return
   * @throws IOException
   */
  public File createTempDirectory(final String prefix) throws IOException;


}
