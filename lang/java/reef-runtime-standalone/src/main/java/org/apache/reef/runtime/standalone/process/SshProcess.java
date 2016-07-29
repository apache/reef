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
package org.apache.reef.runtime.standalone.process;

import com.jcraft.jsch.*;
import org.apache.reef.runtime.local.process.ReefRunnableProcessObserver;
import org.apache.reef.runtime.local.process.RunnableProcess;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A runnable class that encapsulates a process.
 */
public final class SshProcess extends RunnableProcess {

  private static final Logger LOG = Logger.getLogger(SshProcess.class.getName());

  private Session remoteSession;

  private String remoteHostName;

  private final String nodeFolder;

  /**
   * @param command               the command to execute.
   * @param id                    The ID of the process. This is used to name files and in the logs created
   *                              by this process.
   * @param folder                The folder in which this will store its stdout and stderr output
   * @param standardOutFileName   The name of the file used for redirecting STDOUT
   * @param standardErrorFileName The name of the file used for redirecting STDERR
   */
  public SshProcess(final List<String> command,
                    final String id,
                    final File folder,
                    final ReefRunnableProcessObserver processObserver,
                    final String standardOutFileName,
                    final String standardErrorFileName,
                    final Session remoteSession,
                    final String remoteHostName,
                    final String nodeFolder) {
    super(command, id, folder, processObserver, standardOutFileName, standardErrorFileName);
    this.remoteSession = remoteSession;
    this.remoteHostName = remoteHostName;
    this.nodeFolder = nodeFolder;
    LOG.log(Level.FINEST, "SshProcess ready");
  }

  public String getRemoteHomePath() {
    final String getHomeCommand = "pwd";
    try {
      final Channel channel = this.remoteSession.openChannel("exec");
      ((ChannelExec) channel).setCommand(getHomeCommand);
      channel.setInputStream(null);
      final InputStream stdout = channel.getInputStream();
      channel.connect();

      byte[] tmp = new byte[1024];
      StringBuilder homePath = new StringBuilder();
      while (true) {
        while (stdout.available() > 0) {
          final int len = stdout.read(tmp, 0, 1024);
          if (len < 0) {
            break;
          }
          homePath = homePath.append(new String(tmp, 0, len, StandardCharsets.UTF_8));
        }
        if (channel.isClosed()) {
          if (stdout.available() > 0) {
            continue;
          }
          break;
        }
      }
      return homePath.toString().trim();
    } catch (final JSchException | IOException ex) {
      throw new RuntimeException("Unable to retrieve home directory from " +
          this.remoteHostName + " with the pwd command", ex);
    }
  }

  public String getRemoteAbsolutePath() {
    return getRemoteHomePath() + "/" + this.nodeFolder + "/" + super.getId();
  }

  public List<String> getRemoteCommand() {
    final List<String> commandPrefix = new ArrayList<>(Arrays.asList("ssh", this.remoteHostName,
        "cd", this.getRemoteAbsolutePath(), "&&"));
    commandPrefix.addAll(super.getCommand());
    return commandPrefix;
  }

}
