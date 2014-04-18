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
package com.microsoft.reef.runtime.common.client;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.proto.ClientRuntimeProtocol.JobSubmissionProto;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

import javax.inject.Inject;
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

  /**
   * @param jobSubmissionHandler
   * @param runningJobs
   * @param jobSubmissionHelper
   * @param jobStatusMessageHandler  is passed only to make sure it is instantiated
   * @param clientWireUp
   */
  @Inject
  REEFImplementation(final JobSubmissionHandler jobSubmissionHandler,
                     final RunningJobs runningJobs,
                     final JobSubmissionHelper jobSubmissionHelper,
                     final JobStatusMessageHandler jobStatusMessageHandler,
                     final ClientWireUp clientWireUp) {
    this.jobSubmissionHandler = jobSubmissionHandler;
    this.runningJobs = runningJobs;
    this.jobSubmissionHelper = jobSubmissionHelper;
    this.clientWireUp = clientWireUp;
    clientWireUp.performWireUp();
  }

  @Override
  public final void close() {
    this.runningJobs.closeAllJobs();
    this.clientWireUp.close();
  }

  @Override
  public void submit(final Configuration driverConf) {
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

  @Override
  public String getVersion() {
    return this.jobSubmissionHelper.getVersion();
  }


  @NamedParameter(doc = "The driver remote identifier.")
  public final static class DriverRemoteIdentifier implements Name<String> {
  }


}
