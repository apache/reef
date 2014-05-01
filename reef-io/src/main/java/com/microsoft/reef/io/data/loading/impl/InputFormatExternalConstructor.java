/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.data.loading.impl;

import javax.inject.Inject;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;


/**
 * A Tang External Constructor to inject the required
 * InputFormat
 */
@DriverSide
public class InputFormatExternalConstructor implements ExternalConstructor<InputFormat<?,?>> {
  
  @NamedParameter
  public static final class SerializedJobConf implements Name<String> { }
  
  private final JobConf jobConf;
  private final InputFormat<?,?> inputFormat;
  
  @Inject
  public InputFormatExternalConstructor(@Parameter(SerializedJobConf.class) String serializedJobConf) {
    jobConf = WritableSerializer.deserialize(serializedJobConf);
    inputFormat = jobConf.getInputFormat();
  }

  @Override
  public InputFormat<?,?> newInstance() {
    return inputFormat;
  }

}
