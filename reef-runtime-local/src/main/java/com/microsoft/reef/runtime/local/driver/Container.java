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
package com.microsoft.reef.runtime.local.driver;

import com.microsoft.reef.annotations.audience.Private;

import java.io.File;
import java.util.List;

/**
 * Represents a Container: A slice of a machine.
 * <p/>
 * In the case of the local runtime, this slice is always the one of the machine where the job was submitted.
 */
@Private
interface Container extends AutoCloseable {

  /**
   * Run the given commandLine in the container.
   *
   * @param commandLine
   */
  public void run(final List<String> commandLine);

  /**
   * Copies the files to the working directory of the container.
   *
   * @param files
   */
  public void addFiles(final Iterable<File> files);

  /**
   * @return true if the Container is currently executing, false otherwise.
   */
  public boolean isRunning();

  /**
   * @return the ID of the node this Container is executing on.
   */
  public String getNodeID();

  /**
   * @return the ID of this Container.
   */
  public String getContainerID();

  /**
   * Access the main memory available to the Container.
   *
   * @return
   */
  public int getMemory();

  /**
   * @return the working directory of the Container.
   */
  public File getFolder();

  /**
   * Kills the Container.
   */
  @Override
  public void close();
}
