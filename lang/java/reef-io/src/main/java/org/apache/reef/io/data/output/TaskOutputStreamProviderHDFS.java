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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implementation of {@link TaskOutputStreamProvider}.
 * It provides FileOutputStreams on HDFS.
 */
@TaskSide
public final class TaskOutputStreamProviderHDFS extends TaskOutputStreamProvider {

  /**
   * Path of the output directory on HDFS to write outputs.
   */
  private final String outputPath;

  /**
   * HDFS File system.
   */
  private FileSystem fs;

  /**
   * Constructor - instantiated via TANG.
   *
   * @param outputPath path of the output directory on HDFS to write outputs.
   */
  @Inject
  private TaskOutputStreamProviderHDFS(
      @Parameter(TaskOutputService.OutputPath.class) final String outputPath) throws IOException {
    this.outputPath = outputPath;
    final JobConf jobConf = new JobConf();
    fs = FileSystem.get(jobConf);
  }

  /**
   * create a file output stream using the given name.
   * The path of the file on HDFS is 'outputPath/name/taskId'.
   *
   * @param name name of the created output stream
   *             It is used as the name of the directory if the created output stream is a file output stream
   * @return OutputStream to a file on HDFS. The path of the file is 'outputPath/name/taskId'
   * @throws java.io.IOException
   */
  @Override
  public DataOutputStream create(final String name) throws IOException {
    final String directoryPath = outputPath + Path.SEPARATOR + name;
    if (!fs.exists(new Path(directoryPath))) {
      fs.mkdirs(new Path(directoryPath));
    }
    return fs.create(new Path(directoryPath + Path.SEPARATOR + getTaskId()));
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }
}
