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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JobConfExternalConstructor implements ExternalConstructor<JobConf> {

  private static final Logger LOG = Logger.getLogger(JobConfExternalConstructor.class.getName());

  private final String inputFormatClassName;
  private final String inputPath;

  @NamedParameter()
  public static final class InputFormatClass implements Name<String> {
  }

  @NamedParameter(default_value = "NULL")
  public static final class InputPath implements Name<String> {
  }

  @Inject
  public JobConfExternalConstructor(
      final @Parameter(InputFormatClass.class) String inputFormatClassName,
      final @Parameter(InputPath.class) String inputPath) {
    this.inputFormatClassName = inputFormatClassName;
    this.inputPath = inputPath;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public JobConf newInstance() {

    final JobConf jobConf = new JobConf();

    try {

      final Class<? extends InputFormat> inputFormatClass =
          (Class<? extends InputFormat>) Class.forName(this.inputFormatClassName);

      jobConf.setInputFormat(inputFormatClass);

      final Method addInputPath =
          inputFormatClass.getMethod("addInputPath", JobConf.class, Path.class);

      addInputPath.invoke(inputFormatClass, jobConf, new Path(this.inputPath));

    } catch (final ClassNotFoundException ex) {
      throw new RuntimeException("InputFormat: " + this.inputFormatClassName
          + " ClassNotFoundException while creating newInstance of JobConf", ex);
    } catch (final InvocationTargetException | IllegalAccessException ex) {
      throw new RuntimeException("InputFormat: " + this.inputFormatClassName
          + ".addInputPath() method exists, but cannot be called.", ex);
    } catch (final NoSuchMethodException ex) {
      LOG.log(Level.INFO, "{0}.addInputPath() method does not exist", this.inputFormatClassName);
    }

    return jobConf;
  }
}
