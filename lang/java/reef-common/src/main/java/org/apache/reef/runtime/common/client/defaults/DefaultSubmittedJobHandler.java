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
package org.apache.reef.runtime.common.client.defaults;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.SubmittedJob;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Default event handler for SubmittedJob: Log the new job ID. */
@Provided
@ClientSide
public final class DefaultSubmittedJobHandler implements EventHandler<SubmittedJob> {

  private static final Logger LOG = Logger.getLogger(DefaultSubmittedJobHandler.class.getName());

  @Inject
  private DefaultSubmittedJobHandler() { }

  @Override
  public void onNext(final SubmittedJob job) {
    LOG.log(Level.INFO, "Job Submitted: {0}", job);
  }
}
