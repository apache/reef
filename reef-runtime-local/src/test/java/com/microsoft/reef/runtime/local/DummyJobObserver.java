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
package com.microsoft.reef.runtime.local;

import com.microsoft.reef.client.*;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

final class DummyJobObserver implements JobObserver {

  @Inject
  private DummyJobObserver() {
  }

  public static Configuration getConfiguration() {
    try {
      final JavaConfigurationBuilder b = Tang.Factory.getTang().newConfigurationBuilder();
      b.bindImplementation(JobObserver.class, DummyJobObserver.class);
      return b.build();
    } catch (final BindException ex) {
      Logger.getLogger(DummyJobObserver.class.getName()).log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void onNext(final JobMessage message) {
    Logger.getLogger(DummyJobObserver.class.getName()).log(Level.INFO, "JobMessage: {0}", message.toString());
  }

  @Override
  public void onNext(final RunningJob job) {
    Logger.getLogger(DummyJobObserver.class.getName()).log(Level.INFO, "RunningJob {0}", job.toString());
  }

  @Override
  public void onError(final FailedJob job) {
    Logger.getLogger(DummyJobObserver.class.getName()).log(Level.INFO, "JobException: {0}", job.getJobException().toString());
  }

  @Override
  public void onNext(final CompletedJob job) {
    Logger.getLogger(DummyJobObserver.class.getName()).log(Level.INFO, "CompletedJob {0}", job.toString());
  }
}
