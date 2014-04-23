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
package com.microsoft.reef.io.data.loading.api;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;

/**
 * All data loading services should implement
 * this interface
 */
@DriverSide
public interface DataLoadingService {
  
  /**
   * Access to the number of partitions suggested by this DataSource.
   *
   * @return the number of partitions suggested by this DataSource.
   */
  public int getNumberOfPartitions();

  /**
   * @param descriptor
   * @return the configuration for the given Evaluator.
   * @throws BindException
   */
  public Configuration getConfiguration(final AllocatedEvaluator allocatedEvaluator);
  
  /**
   * @return Return the prefix to be used to enumerate
   * context ids for compute requests fired other than
   * the data load contexts
   */
  public String getComputeContextIdPrefix();
  
  /**
   * Distinguishes data loaded contexts from compute
   * contexts.
   * @param context
   * @return true if this context has been loaded with data
   */
  public boolean isDataLoadedContext(ActiveContext context);
}
