/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.yarn.client.uploader;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class that represents a folder on the destination filesystem.
 */
public final class JobFolder {

  private static final Logger LOG = Logger.getLogger(JobFolder.class.getName());
  private final FileSystem fileSystem;
  private final Path path;

  /**
   * This constructor is only called by JobUploader.
   *
   * @param fileSystem
   * @param path
   * @throws IOException when the given path can't be created on the given FileSystem
   */
  JobFolder(final FileSystem fileSystem, final Path path) throws IOException {
    this.fileSystem = fileSystem;
    this.path = path;
    this.fileSystem.mkdirs(this.path);
  }

  /**
   * Uploads the given file to the DFS.
   *
   * @param localFile
   * @return the Path representing the file on the DFS.
   * @throws IOException
   */
  public Path upload(final File localFile) throws IOException {
    if (!localFile.exists()) {
      throw new FileNotFoundException(localFile.getAbsolutePath());
    }

    final Path source = new Path(localFile.getAbsolutePath());
    final Path destination = new Path(this.path, localFile.getName());
    try {
      this.fileSystem.copyFromLocalFile(source, destination);
    } catch (final IOException e) {
      LOG.log(Level.SEVERE, "Unable to upload {0} to {1}", new Object[]{source, destination});
      throw e;
    }
    LOG.log(Level.FINE, "Uploaded {0} to {1}", new Object[]{source, destination});

    return destination;
  }

  /**
   * Shortcut to first upload the file and then form a LocalResource for the YARN submission.
   *
   * @param localFile
   * @return
   * @throws IOException
   */
  public LocalResource uploadAsLocalResource(final File localFile, final LocalResourceType type) throws IOException {
    final Path p = upload(localFile);
    return getLocalResourceForPath(p, type);
  }

  /**
   * Creates a LocalResource instance for the JAR file referenced by the given Path.
   */
  public LocalResource getLocalResourceForPath(final Path jarPath, final LocalResourceType type) throws IOException {
    final LocalResource localResource = Records.newRecord(LocalResource.class);
    final FileStatus status = FileContext.getFileContext(fileSystem.getUri()).getFileStatus(jarPath);
    localResource.setType(type);
    localResource.setVisibility(LocalResourceVisibility.APPLICATION);
    localResource.setResource(ConverterUtils.getYarnUrlFromPath(status.getPath()));
    localResource.setTimestamp(status.getModificationTime());
    localResource.setSize(status.getLen());
    return localResource;
  }

  /**
   * @return the Path on the DFS represented by this object.
   */
  public Path getPath() {
    return this.path;
  }
}
