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
package com.microsoft.reef.runtime.local.client;

import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.OptionalParameter;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents the files added to a driver.
 * <p/>
 * This class is constructed via the from() method that instantiates it based on a JobSubmissionProto
 */
final class DriverFiles {
  private static final Logger LOG = Logger.getLogger(DriverFiles.class.getName());
  private final FileSet localFiles = new FileSet();
  private final FileSet localLibs = new FileSet();
  private final FileSet globalFiles = new FileSet();
  private final FileSet globalLibs = new FileSet();

  private DriverFiles() {
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

  private void checkFile(final File f) throws IOException {
    if (this.globalLibs.containsFileWithName(f.getName())) {
      LOG.log(Level.FINEST, "Adding a file that is already part of the global libraries: " + f);
    }
    if (this.globalFiles.containsFileWithName(f.getName())) {
      LOG.log(Level.FINEST, "Adding a file that is already part of the global files: " + f);
    }
    if (this.localLibs.containsFileWithName(f.getName())) {
      LOG.log(Level.FINEST, "Adding a file that is already part of the local libraries: " + f);
    }
    if (this.localFiles.containsFileWithName(f.getName())) {
      LOG.log(Level.FINEST, "Adding a file that is already part of the local files: " + f);
    }

  }

  /**
   * Assembles the classpath to be used for the driver.
   * <p/>
   * Local classes come first, global ones after that. Both local and global are sorted beforehand.
   *
   * @return the classpath to be used for the driver.
   */
  public final String getClassPath() {
    final List<String> localList = new ArrayList<>(localLibs.fileNames());
    Collections.sort(localList);
    final List<String> globalList = new ArrayList<>(globalLibs.fileNames());
    Collections.sort(globalList);
    final List classPathList = new ArrayList();
    classPathList.addAll(localList);
    classPathList.addAll(globalList);
    return StringUtils.join(classPathList, File.pathSeparatorChar);
  }

  /**
   * Copies this set of files to the destination folder given.
   *
   * @param destinationFolder
   * @throws IOException
   */
  public void copyTo(final File destinationFolder) throws IOException {
    destinationFolder.mkdirs();
    this.localFiles.copyTo(destinationFolder);
    this.localLibs.copyTo(destinationFolder);
    this.globalLibs.copyTo(destinationFolder);
    this.globalFiles.copyTo(destinationFolder);
  }

  /**
   * Fils out a ConfigurationModule.
   *
   * @param input
   * @param globalFileField the field on which to set() the global files.
   * @param globalLibField  the field on which to set() the global libraries.
   * @param localFileField  the field on which to set() the local files.
   * @param localLibField   the field on which to set() the local libraries.
   * @return
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

  /**
   * Instantiates an instance based on the given JobSubmissionProto
   *
   * @param jobSubmissionProto
   * @return
   * @throws IOException
   */
  public static DriverFiles fromJobSubmission(final ClientRuntimeProtocol.JobSubmissionProto jobSubmissionProto) throws IOException {
    final DriverFiles driverFiles = new DriverFiles();

    for (final ReefServiceProtos.FileResourceProto frp : jobSubmissionProto.getGlobalFileList()) {
      final File f = new File(frp.getPath());
      if (frp.getType() == ReefServiceProtos.FileType.LIB) {
        driverFiles.addGlobalLib(f);
      } else {
        driverFiles.addGlobalFile(f);
      }

    }
    for (final ReefServiceProtos.FileResourceProto frp : jobSubmissionProto.getLocalFileList()) {
      final File f = new File(frp.getPath());
      if (frp.getType() == ReefServiceProtos.FileType.LIB) {
        driverFiles.addLocalLib(f);
      } else {
        driverFiles.addLocalFile(f);
      }
    }
    return driverFiles;
  }


}
