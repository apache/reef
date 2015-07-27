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
package org.apache.reef.runtime.yarn.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.util.JARFileMaker;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class that creates the JAR file with the global files on the driver and then uploads it to the job folder on
 * (H)DFS.
 */
final class GlobalJarUploader implements Callable<Map<String, LocalResource>> {

  private static final Logger LOG = Logger.getLogger(GlobalJarUploader.class.getName());

  /**
   * Used for the file system constants.
   */
  private final REEFFileNames fileNames;

  /**
   * Utility to actually perform the update.
   */
  private final UploaderToJobFolder uploader;

  /**
   * True, if the global JAR has already been uploaded.
   */
  private boolean isUploaded;

  /**
   * Path to the uploaded global JAR.
   */
  private Path pathToGlobalJar;

  /**
   * The cached LocalResource for global JAR. The latest global JAR
   * is still updated due to REEF-348.
   * This is primarily used to detect change in JAR timestamps.
   */
  private LocalResource globalJarResource;

  @Inject
  GlobalJarUploader(final REEFFileNames fileNames,
                    final UploaderToJobFolder uploader) {
    this.fileNames = fileNames;
    this.uploader = uploader;
  }

  /**
   * Creates the JAR file with the global files on the driver and then uploads it to the job folder on
   * (H)DFS.
   *
   * @return the map to be used as the "global" resources when submitting Evaluators.
   * @throws IOException if the creation of the JAR or the upload fails
   */
  @Override
  public synchronized Map<String, LocalResource> call() throws IOException {
    final Map<String, LocalResource> globalResources = new HashMap<>(1);
    if (!this.isUploaded){
      this.pathToGlobalJar = this.uploader.uploadToJobFolder(makeGlobalJar());
      this.isUploaded = true;
    }

    final LocalResource updatedGlobalJarResource = this.uploader.makeLocalResourceForJarFile(this.pathToGlobalJar);

    if (this.globalJarResource != null
        && this.globalJarResource.getTimestamp() != updatedGlobalJarResource.getTimestamp()) {
      LOG.log(Level.WARNING,
              "The global JAR LocalResource timestamp has been changed from "
              + this.globalJarResource.getTimestamp() + " to " + updatedGlobalJarResource.getTimestamp());
    }

    this.globalJarResource = updatedGlobalJarResource;

    // For now, always rewrite the information due to REEF-348
    globalResources.put(this.fileNames.getGlobalFolderPath(), updatedGlobalJarResource);

    return globalResources;
  }

  /**
   * Creates the JAR file for upload.
   *
   * @return
   * @throws IOException
   */
  private File makeGlobalJar() throws IOException {
    final File jarFile = new File(this.fileNames.getGlobalFolderName() + this.fileNames.getJarFileSuffix());
    new JARFileMaker(jarFile).addChildren(this.fileNames.getGlobalFolder()).close();
    return jarFile;
  }
}
