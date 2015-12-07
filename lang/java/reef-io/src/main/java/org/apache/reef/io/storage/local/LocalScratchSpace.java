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
package org.apache.reef.io.storage.local;

import org.apache.reef.io.storage.ScratchSpace;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

public class LocalScratchSpace implements ScratchSpace {

  private static final Logger LOG = Logger.getLogger(LocalScratchSpace.class.getName());

  private final String jobName;
  private final String evaluatorName;
  private final Set<File> tempFiles = new ConcurrentSkipListSet<>();
  /**
   * Zero denotes "unlimited".
   */
  private long quota;

  public LocalScratchSpace(final String jobName, final String evaluatorName) {
    this.jobName = jobName;
    this.evaluatorName = evaluatorName;
    this.quota = 0;
  }

  public LocalScratchSpace(final String jobName, final String evaluatorName, final long quota) {
    this.jobName = jobName;
    this.evaluatorName = evaluatorName;
    this.quota = quota;
  }

  public File newFile() {
    final File ret;
    try {
      ret = File.createTempFile("reef-" + jobName + "-" + evaluatorName,
          "tmp");
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    tempFiles.add(ret);
    return ret;
  }

  @Override
  public long availableSpace() {
    return quota;
  }

  @Override
  public long usedSpace() {
    long ret = 0;
    for (final File f : tempFiles) {
      try {
        ret += f.length();
      } catch (final SecurityException e) {
        LOG.info("Fail to get file info:" + f.getAbsolutePath());
      }
    }
    return ret;
  }

  @Override
  public void delete() {
    for (final File f : tempFiles) {
      try {
        if (!f.delete()) {
          f.deleteOnExit();
        }
      } catch (final SecurityException e) {
        throw new RuntimeException("Fail to delete file:" + f.getAbsolutePath(), e);
      }
    }
    tempFiles.clear();
  }

}
