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
package org.apache.reef.io.network.group.config;

import org.apache.reef.wake.ComparableIdentifier;

import java.util.List;

public interface TaskTree {

  public static enum Status {
    UNSCHEDULED, SCHEDULED, COMPLETED, ANY;
    // ANY is to be used for search only. Its not an actual state
  }

  void add(ComparableIdentifier id);

  ComparableIdentifier parent(ComparableIdentifier id);

  ComparableIdentifier left(ComparableIdentifier id);

  ComparableIdentifier right(ComparableIdentifier id);

  List<ComparableIdentifier> neighbors(ComparableIdentifier id);

  List<ComparableIdentifier> children(ComparableIdentifier id);

  int childrenSupported(ComparableIdentifier taskId);

  void remove(ComparableIdentifier failedTaskId);

  List<ComparableIdentifier> scheduledChildren(ComparableIdentifier taskId);

  List<ComparableIdentifier> scheduledNeighbors(ComparableIdentifier taskId);

  void setStatus(ComparableIdentifier taskId, Status status);

  Status getStatus(ComparableIdentifier taskId);
}
