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
package com.microsoft.reef.io.data.loading.impl;

import javax.inject.Inject;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.microsoft.reef.annotations.audience.TaskSide;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

/**
 * A Tang external constructor to inject an InputSplit
 * by deserializing the serialized input split assigned
 * to this evaluator
 */
@TaskSide
public class InputSplitExternalConstructor implements ExternalConstructor<InputSplit> {

  @NamedParameter
  public static final class SerializedInputSplit implements Name<String> { }

  private final InputSplit inputSplit;

  @Inject
  public InputSplitExternalConstructor(
      final JobConf jobConf,
      @Parameter(SerializedInputSplit.class) final String serializedInputSplit){
    this.inputSplit = WritableSerializer.deserialize(serializedInputSplit, jobConf);
  }

  @Override
  public InputSplit newInstance() {
    return inputSplit;
  }

}
