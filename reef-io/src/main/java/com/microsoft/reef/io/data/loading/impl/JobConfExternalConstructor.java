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

import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import javax.inject.Inject;

/**
 *
 */
public class JobConfExternalConstructor implements ExternalConstructor<JobConf> {

  private final String inputFormatClass;
  private final String inputPath;

  @NamedParameter()
  public static final class InputFormatClass implements Name<String> {
  }

  @NamedParameter(default_value = "NULL")
  public static final class InputPath implements Name<String> {
  }

  @Inject
  public JobConfExternalConstructor(
      @Parameter(InputFormatClass.class) final String inputFormatClass,
      @Parameter(InputPath.class) final String inputPath) {
    this.inputFormatClass = inputFormatClass;
    this.inputPath = inputPath;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public JobConf newInstance() {
    final JobConf jobConf = new JobConf();
    try {
      jobConf.setInputFormat((Class<? extends InputFormat>) Class.forName(inputFormatClass));
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("InputFormat: " + inputFormatClass
          + " ClassNotFoundException while creating newInstance of JobConf", e);
    }
    if (inputFormatClass.equals(TextInputFormat.class.getName())) {
      TextInputFormat.addInputPath(jobConf, new Path(inputPath));
    }
    return jobConf;
  }

}
