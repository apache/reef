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
package org.apache.reef.runtime.common.driver.client;

import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A handler for job status messages that just logs them.
 */
final class LoggingJobStatusHandler implements EventHandler<ReefServiceProtos.JobStatusProto> {
  private static final Logger LOG = Logger.getLogger(LoggingJobStatusHandler.class.getName());

  @Inject
  LoggingJobStatusHandler() {
  }

  @Override
  public void onNext(final ReefServiceProtos.JobStatusProto jobStatusProto) {
    LOG.log(Level.INFO, "Received a JobStatus message that can't be sent:\n" + jobStatusProto.toString());
  }
}
