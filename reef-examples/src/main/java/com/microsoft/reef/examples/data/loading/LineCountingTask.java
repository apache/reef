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
package com.microsoft.reef.examples.data.loading;

import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.TaskSide;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.task.Task;

/**
 * The task that iterates over the 
 * data set to count the number of records
 * Assumes a TextInputFormat and that
 * records represent lines
 */
@TaskSide
public class LineCountingTask implements Task {
  private static final Logger LOG = Logger.getLogger(LineCountingTask.class.getName());
  
  private final DataSet<?,?> dataSet;
  
  @Inject
  public LineCountingTask(final DataSet<?,?> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public byte[] call(final byte[] arg0) throws Exception {
    int numEx = 0;
    for (final Pair<?,?> keyValue : dataSet) {
      LOG.finest(keyValue.toString());
      ++numEx;
    }
    return Integer.toString(numEx).getBytes();
  }

}