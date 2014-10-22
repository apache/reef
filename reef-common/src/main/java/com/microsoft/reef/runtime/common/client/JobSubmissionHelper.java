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
package com.microsoft.reef.runtime.common.client;

import com.microsoft.reef.client.REEF;
import com.microsoft.reef.driver.parameters.*;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.util.JARFileMaker;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shared code between job submission with and without client
 */
final class JobSubmissionHelper {

  static {
    System.out.println(
        "\nPowered by\n" +
            "     ___________  ______  ______  _______\n" +
            "    /  ______  / /  ___/ /  ___/ /  ____/\n" +
            "   /     _____/ /  /__  /  /__  /  /___\n" +
            "  /  /\\  \\     /  ___/ /  ___/ /  ____/\n" +
            " /  /  \\  \\   /  /__  /  /__  /  /\n" +
            "/__/    \\__\\ /_____/ /_____/ /__/\n\n" +
            "From Microsoft CISL\n"
    );
  }

  private static final Logger LOG = Logger.getLogger(JobSubmissionHelper.class.getName());

  private final ConfigurationSerializer configurationSerializer;

  @Inject
  JobSubmissionHelper(ConfigurationSerializer configurationSerializer) {
    this.configurationSerializer = configurationSerializer;
  }

  /**
   * Fils out a JobSubmissionProto based on the driver configuration given.
   *
   * @param driverConfiguration
   * @return
   * @throws InjectionException
   * @throws IOException
   */
  final ClientRuntimeProtocol.JobSubmissionProto.Builder getJobsubmissionProto(final Configuration driverConfiguration) throws InjectionException, IOException {
    final Injector injector = Tang.Factory.getTang().newInjector(driverConfiguration);

    final ClientRuntimeProtocol.JobSubmissionProto.Builder jbuilder = ClientRuntimeProtocol.JobSubmissionProto.newBuilder()
        .setIdentifier(injector.getNamedInstance(DriverIdentifier.class))
        .setDriverMemory(injector.getNamedInstance(DriverMemory.class))
        .setUserName(System.getProperty("user.name"))
        .setConfiguration(configurationSerializer.toString(driverConfiguration));


    for (final String globalFileName : injector.getNamedInstance(JobGlobalFiles.class)) {
      LOG.log(Level.FINEST, "Adding global file: {0}", globalFileName);
      jbuilder.addGlobalFile(getFileResourceProto(globalFileName, ReefServiceProtos.FileType.PLAIN));
    }

    for (final String globalLibraryName : injector.getNamedInstance(JobGlobalLibraries.class)) {
      LOG.log(Level.FINEST, "Adding global library: {0}", globalLibraryName);
      jbuilder.addGlobalFile(getFileResourceProto(globalLibraryName, ReefServiceProtos.FileType.LIB));
    }

    for (final String localFileName : injector.getNamedInstance(DriverLocalFiles.class)) {
      LOG.log(Level.FINEST, "Adding local file: {0}", localFileName);
      jbuilder.addLocalFile(getFileResourceProto(localFileName, ReefServiceProtos.FileType.PLAIN));
    }

    for (final String localLibraryName : injector.getNamedInstance(DriverLocalLibraries.class)) {
      LOG.log(Level.FINEST, "Adding local library: {0}", localLibraryName);
      jbuilder.addLocalFile(getFileResourceProto(localLibraryName, ReefServiceProtos.FileType.LIB));
    }

    return jbuilder;
  }

  /**
   * Turns a pathname into the right protocol for job submission.
   *
   * @param fileName
   * @param type
   * @return
   * @throws IOException
   */
  private ReefServiceProtos.FileResourceProto getFileResourceProto(final String fileName, final ReefServiceProtos.FileType type) throws IOException {
    File file = new File(fileName);
    if (file.exists()) {
      // It is a local file and can be added.
      if (file.isDirectory()) {
        // If it is a directory, create a JAR file of it and add that instead.
        file = toJar(file);
      }
      return ReefServiceProtos.FileResourceProto.newBuilder()
          .setName(file.getName())
          .setPath(file.getPath())
          .setType(type)
          .build();
    } else {
      // The file isn't in the local filesytem. Assume that the file is actually a URI.
      // We then assume that the underlying resource manager knows how to deal with it.
      try {
        final URI uri = new URI(fileName);
        final String path = uri.getPath();
        final String name = path.substring(path.lastIndexOf('/') + 1);
        return ReefServiceProtos.FileResourceProto.newBuilder()
            .setName(name)
            .setPath(uri.toString())
            .setType(type)
            .build();
      } catch (final URISyntaxException e) {
        throw new IOException("Unable to parse URI.", e);
      }
    }
  }

  /**
   * Turns temporary folder "foo" into a jar file "foo.jar"
   *
   * @param file
   * @return
   * @throws IOException
   */
  private File toJar(final File file) throws IOException {
    final File tempFolder = Files.createTempDirectory("reef-tmp-tempFolder").toFile();
    final File jarFile = File.createTempFile(file.getCanonicalFile().getName(), ".jar", tempFolder);
    LOG.log(Level.FINEST, "Adding contents of folder {0} to {1}", new Object[]{file, jarFile});
    try (final JARFileMaker jarMaker = new JARFileMaker(jarFile)) {
      jarMaker.addChildren(file);
    }
    return jarFile;
  }

  /**
   * @return the version string for REEF.
   */
  String getVersion() {
    return REEF.REEF_VERSION;
  }
}
