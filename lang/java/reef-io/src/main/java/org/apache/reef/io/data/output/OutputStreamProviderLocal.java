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
package org.apache.reef.io.data.output;

import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Implementation of {@link org.apache.reef.io.data.output.OutputStreamProvider}.
 * It provides FileOutputStreams on the local file system.
 */
public final class OutputStreamProviderLocal implements OutputStreamProvider {

  /**
   * Path of the output directory on the local disk to write outputs.
   */
  private final String outputPath;

  /**
   * Id of the current task.
   */
  private String taskId;

  /**
   * Constructor - instantiated via TANG.
   *
   * @param outputPath path of the output directory on the local disk to write outputs.
   */
  @Inject
  private OutputStreamProviderLocal(
      @Parameter(OutputService.OutputPath.class) final String outputPath) {
    this.outputPath = outputPath;
  }

  /**
   * create a file output stream using the given name.
   * The path of the file on the local file system is 'outputPath/name/taskId'.
   *
   * @param name name of the created output stream
   *             It is used as the name of the directory if the created output stream is a file output stream
   * @return OutputStream to a file on local file system. The path of the file is 'outputPath/name/taskId'
   * @throws java.io.IOException
   */
  @Override
  public DataOutputStream create(final String name) throws IOException {
    final String directoryPath = outputPath + File.separator + name;
    final File directory = new File(directoryPath);

    synchronized (OutputStreamProviderLocal.class) {
      if (!directory.exists()) {
        directory.mkdirs();
      }
    }

    final File file = new File(directoryPath + File.separator + taskId);
    return new DataOutputStream(new FileOutputStream(file));
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Set task id, which is used as a part of the output stream file path.
   *
   * @param taskId id of the current task
   */
  @Override
  public void setTaskId(final String taskId) {
    this.taskId = taskId;
  }
}
