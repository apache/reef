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
package com.microsoft.reef.driver.evaluator;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.ContextAndActivitySubmittable;
import com.microsoft.reef.driver.ContextSubmittable;
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.io.naming.Identifiable;
import com.microsoft.tang.Configuration;

import java.io.File;
import java.io.IOException;

/**
 * Represents an Evaluator that is allocated, but is not running yet.
 */
@Public
@DriverSide
@Provided
public interface AllocatedEvaluator extends AutoCloseable, Identifiable, ContextSubmittable, ContextAndActivitySubmittable {


  /**
   * Puts the given file into the working directory of the Evaluator.
   *
   * @param file the file to be copied
   * @throws IOException if the copy fails.
   */
  public void addFile(final File file);

  /**
   * Puts the given file into the working directory of the Evaluator and adds it to its classpath.
   *
   * @param file the file to be copied
   * @throws IOException if the copy fails.
   */
  public void addLibrary(final File file);

  /**
   * Releases the allocated evaluator back to the resource manager.
   */
  @Override
  public void close();

  /**
   * @return the node descriptor of the physical environment on this evaluator.
   */
  public NodeDescriptor getNodeDescriptor();

  @Override
  public void submitContext(final Configuration contextConfiguration);

  @Override
  public void submitContextAndService(final Configuration contextConfiguration,
                                      final Configuration serviceConfiguration);

  @Override
  public void submitContextAndActivity(final Configuration contextConfiguration,
                                       final Configuration activityConfiguration);

  @Override
  public void submitContextAndServiceAndActivity(final Configuration contextConfiguration,
                                                 final Configuration serviceConfiguration,
                                                 final Configuration activityConfiguration);

  /**
   * Set the type of Evaluator to be instantiated. Defaults to EvaluatorType.JVM.
   *
   * @param type
   */
  public void setType(final EvaluatorType type);


  /**
   * @deprecated Use submitContext() instead.
   */
  @Deprecated
  public void submit(final Configuration contextConfiguration);

  /**
   * @deprecated use submitContextAndActivity() instead.
   */
  @Deprecated
  public void submit(final Configuration contextConfiguration, final Configuration activityConfiguration);

  /**
   * Puts the given resource into the working directory of the Evaluator.
   * <p/>
   * If its name ends in ".jar", it will be added to the class path for the
   * Evaluator.
   *
   * @param file that will be added to the local directory of this evaluator
   * @throws IOException
   * @deprecated use addFile and addLibrary instead.
   */
  public void addFileResource(final File file) throws IOException;

}