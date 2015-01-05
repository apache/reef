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
package org.apache.reef.runtime.common.evaluator;

import org.apache.reef.util.OSUtils;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This Handler writes the Process ID (PID) to a file with a name given in PID_FILE_NAME to the local working directory.
 */
public class PIDStoreStartHandler implements EventHandler<StartTime> {
  public static final String PID_FILE_NAME = "PID.txt";
  private static final Logger LOG = Logger.getLogger(PIDStoreStartHandler.class.getName());

  @Inject
  public PIDStoreStartHandler() {
  }

  @Override
  public void onNext(final StartTime startTime) {
    final long pid = OSUtils.getPID();
    final File outfile = new File(PID_FILE_NAME);
    LOG.log(Level.FINEST, "Storing pid `" + pid + "` in file " + outfile.getAbsolutePath());
    try (final PrintWriter p = new PrintWriter((new FileOutputStream(PID_FILE_NAME)))) {
      p.write(String.valueOf(pid));
      p.write("\n");
    } catch (final FileNotFoundException e) {
      LOG.log(Level.WARNING, "Unable to create PID file.", e);
    }
  }
}
