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
package org.apache.reef.runtime.common.launch;

import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.profiler.WakeProfiler;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An EventHandler that writes out the profiler results.
 */
public final class ProfilingStopHandler implements EventHandler<StopTime> {
  private static final Logger LOG = Logger.getLogger(ProfilingStopHandler.class.getName());
  private static WakeProfiler profiler;
  private final String launchID;

  @Inject
  public ProfilingStopHandler(@Parameter(LaunchID.class) final String launchID) {
    this.launchID = launchID;
  }

  public static void setProfiler(final WakeProfiler profiler) {
    ProfilingStopHandler.profiler = profiler;
  }

  @Override
  public void onNext(final StopTime stopTime) {
    try (PrintWriter out = new PrintWriter("profile-" + launchID + ".json", "UTF-8")) {
      out.print(profiler.objectGraphToString());
    } catch (final FileNotFoundException | UnsupportedEncodingException e) {
      LOG.log(Level.WARNING, "Unable to write the profile", e);
    }
  }
}
