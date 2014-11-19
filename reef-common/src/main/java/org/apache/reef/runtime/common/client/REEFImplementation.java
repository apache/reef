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
package org.apache.reef.runtime.common.client;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.client.REEF;
import org.apache.reef.proto.ClientRuntimeProtocol.JobSubmissionProto;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.util.REEFVersion;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@ClientSide
@Provided
@Private
public final class REEFImplementation implements REEF {

  private final static Logger LOG = Logger.getLogger(REEFImplementation.class.getName());

  private final JobSubmissionHandler jobSubmissionHandler;
  private final RunningJobs runningJobs;
  private final JobSubmissionHelper jobSubmissionHelper;
  private final ClientWireUp clientWireUp;
  private final LoggingScopeFactory loggingScopeFactory;

  /**
   * @param jobSubmissionHandler
   * @param runningJobs
   * @param jobSubmissionHelper
   * @param jobStatusMessageHandler is passed only to make sure it is instantiated
   * @param clientWireUp
   * @param reefVersion             provides the current version of REEF.
   */
  @Inject
  REEFImplementation(final JobSubmissionHandler jobSubmissionHandler,
                     final RunningJobs runningJobs,
                     final JobSubmissionHelper jobSubmissionHelper,
                     final JobStatusMessageHandler jobStatusMessageHandler,
                     final ClientWireUp clientWireUp,
                     final LoggingScopeFactory loggingScopeFactory,
                     final REEFVersion reefVersion) {
    this.jobSubmissionHandler = jobSubmissionHandler;
    this.runningJobs = runningJobs;
    this.jobSubmissionHelper = jobSubmissionHelper;
    this.clientWireUp = clientWireUp;
    clientWireUp.performWireUp();
    this.loggingScopeFactory = loggingScopeFactory;
    reefVersion.logVersion();
  }

  @Override
  public final void close() {
    this.runningJobs.closeAllJobs();
    this.clientWireUp.close();
  }

  @Override
  public void submit(final Configuration driverConf) {
    try (LoggingScope ls = this.loggingScopeFactory.reefSubmit()) {
      final JobSubmissionProto submissionMessage;
      try {
        if (this.clientWireUp.isClientPresent()) {
          submissionMessage = this.jobSubmissionHelper.getJobsubmissionProto(driverConf)
              .setRemoteId(this.clientWireUp.getRemoteManagerIdentifier())
              .build();
        } else {
          submissionMessage = this.jobSubmissionHelper.getJobsubmissionProto(driverConf)
              .setRemoteId(ErrorHandlerRID.NONE)
              .build();
        }
      } catch (final Exception e) {
        throw new RuntimeException("Exception while processing driver configuration.", e);
      }

      this.jobSubmissionHandler.onNext(submissionMessage);
    }
  }

  @NamedParameter(doc = "The driver remote identifier.")
  public final static class DriverRemoteIdentifier implements Name<String> {
  }


}
