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
package org.apache.reef.runtime.local.client;

import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.files.FileResource;
import org.apache.reef.runtime.common.files.FileType;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.OptionalParameter;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the files added to a driver.
 * <p>
 * This class is constructed via the from() method that instantiates it based on a JobSubmissionProto
 */
final class DriverFiles {

  private static final Logger LOG = Logger.getLogger(DriverFiles.class.getName());

  private final FileSet localFiles = new FileSet();
  private final FileSet localLibs = new FileSet();
  private final FileSet globalFiles = new FileSet();
  private final FileSet globalLibs = new FileSet();
  private final REEFFileNames fileNames;

  DriverFiles(final REEFFileNames fileNames) {
    this.fileNames = fileNames;
  }

  /**
   * Instantiates an instance based on the given JobSubmissionProto.
   *
   * @param jobSubmissionEvent the JobSubmissionProto to parse.
   * @return a DriverFiles instance pre-populated with the information from the given JobSubmissionProto.
   * @throws IOException
   */
  public static DriverFiles fromJobSubmission(
      final JobSubmissionEvent jobSubmissionEvent,
      final REEFFileNames fileNames) throws IOException {

    final DriverFiles driverFiles = new DriverFiles(fileNames);

    for (final FileResource frp : jobSubmissionEvent.getGlobalFileSet()) {
      final File f = new File(frp.getPath());
      if (frp.getType() == FileType.LIB) {
        driverFiles.addGlobalLib(f);
      } else {
        driverFiles.addGlobalFile(f);
      }
    }

    for (final FileResource frp : jobSubmissionEvent.getLocalFileSet()) {
      final File f = new File(frp.getPath());
      if (frp.getType() == FileType.LIB) {
        driverFiles.addLocalLib(f);
      } else {
        driverFiles.addLocalFile(f);
      }
    }

    return driverFiles;
  }

  private void addLocalLib(final File f) throws IOException {
    checkFile(f);
    this.localLibs.add(f);
  }

  private void addLocalFile(final File f) throws IOException {
    checkFile(f);
    this.localFiles.add(f);
  }

  private void addGlobalFile(final File f) throws IOException {
    checkFile(f);
    this.globalFiles.add(f);
  }

  private void addGlobalLib(final File f) throws IOException {
    checkFile(f);
    this.globalLibs.add(f);
  }

  private void checkFile(final File f) {
    if (this.globalLibs.containsFileWithName(f.getName())) {
      LOG.log(Level.FINEST, "Adding a file that is already part of the global libraries: {0}", f);
    }
    if (this.globalFiles.containsFileWithName(f.getName())) {
      LOG.log(Level.FINEST, "Adding a file that is already part of the global files: {0}", f);
    }
    if (this.localLibs.containsFileWithName(f.getName())) {
      LOG.log(Level.FINEST, "Adding a file that is already part of the local libraries: {0}", f);
    }
    if (this.localFiles.containsFileWithName(f.getName())) {
      LOG.log(Level.FINEST, "Adding a file that is already part of the local files: {0}", f);
    }
  }

  /**
   * Copies this set of files to the destination folder given.
   * <p>
   * Will attempt to create symbolic links for the files to the destination
   * folder.  If the filesystem does not support symbolic links or the user
   * does not have appropriate permissions, the entire file will be copied instead.
   *
   * @param destinationFolder the folder the files shall be copied to.
   * @throws IOException if one or more of the copies fail.
   */
  public void copyTo(final File destinationFolder) throws IOException {
    if (!destinationFolder.exists() && !destinationFolder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", destinationFolder.getAbsolutePath());
    }
    final File reefFolder = new File(destinationFolder, fileNames.getREEFFolderName());

    final File localFolder = new File(reefFolder, fileNames.getLocalFolderName());
    final File globalFolder = new File(reefFolder, fileNames.getGlobalFolderName());
    if (!localFolder.exists() && !localFolder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", localFolder.getAbsolutePath());
    }
    if (!globalFolder.exists() && !globalFolder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", globalFolder.getAbsolutePath());
    }

    try {
      this.localFiles.createSymbolicLinkTo(localFolder);
      this.localLibs.createSymbolicLinkTo(localFolder);
      this.globalLibs.createSymbolicLinkTo(globalFolder);
      this.globalFiles.createSymbolicLinkTo(globalFolder);
    } catch (final Throwable t) {
      LOG.log(Level.FINE, "Can't symlink the files, copying them instead.", t);
      this.localFiles.copyTo(localFolder);
      this.localLibs.copyTo(localFolder);
      this.globalLibs.copyTo(globalFolder);
      this.globalFiles.copyTo(globalFolder);
    }
  }

  /**
   * Fills out a ConfigurationModule.
   *
   * @param input           The ConfigurationModule to start with.
   * @param globalFileField the field on which to set() the global files.
   * @param globalLibField  the field on which to set() the global libraries.
   * @param localFileField  the field on which to set() the local files.
   * @param localLibField   the field on which to set() the local libraries.
   * @return a copy of input with files and libraries added to the given fields.
   */
  public ConfigurationModule addNamesTo(final ConfigurationModule input,
                                        final OptionalParameter<String> globalFileField,
                                        final OptionalParameter<String> globalLibField,
                                        final OptionalParameter<String> localFileField,
                                        final OptionalParameter<String> localLibField) {
    ConfigurationModule result = input;
    result = this.globalFiles.addNamesTo(result, globalFileField);
    result = this.globalLibs.addNamesTo(result, globalLibField);
    result = this.localFiles.addNamesTo(result, localFileField);
    result = this.localLibs.addNamesTo(result, localLibField);
    return result;
  }
}
