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
package com.microsoft.reef.runtime.common.launch;

import com.microsoft.reef.runtime.common.launch.parameters.LaunchID;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.profiler.WakeProfiler;
import com.microsoft.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An EventHandler that writes out the profiler results.
 */
final class ProfilingStopHandler implements EventHandler<StopTime> {
  private static final Logger LOG = Logger.getLogger(ProfilingStopHandler.class.getName());
  private static WakeProfiler profiler;
  private final String launchID;

  @Inject
  public ProfilingStopHandler(final @Parameter(LaunchID.class) String launchID) {
    this.launchID = launchID;
  }

  static void setProfiler(final WakeProfiler profiler) {
    ProfilingStopHandler.profiler = profiler;
  }

  @Override
  public void onNext(final StopTime stopTime) {
    try (final PrintWriter out = new PrintWriter("profile-" + launchID + ".json")) {
      out.print(profiler.objectGraphToString());
    } catch (final FileNotFoundException e) {
      LOG.log(Level.WARNING, "Unable to write the profile", e);
    }
  }
}
