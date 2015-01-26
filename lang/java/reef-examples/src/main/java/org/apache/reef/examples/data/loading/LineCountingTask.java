/**
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
package org.apache.reef.examples.data.loading;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The task that iterates over the data set to count the number of records.
 * Assumes TextInputFormat and that records represent lines.
 */
@TaskSide
public class LineCountingTask implements Task {

  private static final Logger LOG = Logger.getLogger(LineCountingTask.class.getName());

  private final DataSet<LongWritable, Text> dataSet;

  @Inject
  public LineCountingTask(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.FINER, "LineCounting task started");
    int numEx = 0;
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      // LOG.log(Level.FINEST, "Read line: {0}", keyValue);
      ++numEx;
    }
    LOG.log(Level.FINER, "LineCounting task finished: read {0} lines", numEx);
    return Integer.toString(numEx).getBytes();
  }
}
