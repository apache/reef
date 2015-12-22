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
package org.apache.reef.vortex.common;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Worker-to-Master protocol.
 * A report of Tasklet statuses sent form the {@link org.apache.reef.vortex.evaluator.VortexWorker}
 * to the {@link org.apache.reef.vortex.driver.VortexMaster}.
 */
@Private
@Unstable
@DriverSide
public final class WorkerReport {
  private ArrayList<TaskletReport> taskletReports;

  public WorkerReport(final Collection<TaskletReport> taskletReports) {
    this.taskletReports = new ArrayList<>(taskletReports);
  }

  /**
   * @return the list of Tasklet reports.
   */
  public List<TaskletReport> getTaskletReports() {
    return Collections.unmodifiableList(taskletReports);
  }
}
