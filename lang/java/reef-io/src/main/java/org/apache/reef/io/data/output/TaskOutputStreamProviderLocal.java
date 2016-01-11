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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of {@link TaskOutputStreamProvider}.
 * It provides FileOutputStreams on the local file system.
 */
public final class TaskOutputStreamProviderLocal extends TaskOutputStreamProvider {
  private static final Logger LOG = Logger.getLogger(TaskOutputStreamProviderLocal.class.getName());

  /**
   * Path of the output directory on the local disk to write outputs.
   */
  private final String outputPath;

  /**
   * Constructor - instantiated via TANG.
   *
   * @param outputPath path of the output directory on the local disk to write outputs.
   */
  @Inject
  private TaskOutputStreamProviderLocal(
      @Parameter(TaskOutputService.OutputPath.class) final String outputPath) {
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

    synchronized (TaskOutputStreamProviderLocal.class) {
      if (!directory.exists() && !directory.mkdirs()) {
        LOG.log(Level.WARNING, "Failed to create [{0}]", directory.getAbsolutePath());
      }
    }

    final File file = new File(directoryPath + File.separator + getTaskId());
    return new DataOutputStream(new FileOutputStream(file));
  }

  @Override
  public void close() throws IOException {
  }
}
