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
package org.apache.reef.io.data.loading.api;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.Configuration;

/**
 * All data loading services should implement this interface.
 */
@DriverSide
public interface DataLoadingService {

  /**
   * Access to the number of partitions suggested by this DataSource.
   *
   * @return the number of partitions suggested by this DataSource.
   */
  int getNumberOfPartitions();

  /**
   * @return the context configuration for the given Evaluator.
   */
  Configuration getContextConfiguration(AllocatedEvaluator allocatedEvaluator);

  /**
   * @return the service configuration for the given Evaluator.
   */
  Configuration getServiceConfiguration(AllocatedEvaluator allocatedEvaluator);

  /**
   * @return Return the prefix to be used to enumerate
   * context ids for compute requests fired other than
   * the data load contexts.
   */
  String getComputeContextIdPrefix();

  /**
   * Distinguishes data loaded contexts from compute contexts.
   *
   * @return true if this context has been loaded with data.
   */
  boolean isDataLoadedContext(ActiveContext context);

  /**
   * @return true if this is a computation context,
   * false otherwise. (e.g. this is a data loading context).
   */
  boolean isComputeContext(ActiveContext context);
}
