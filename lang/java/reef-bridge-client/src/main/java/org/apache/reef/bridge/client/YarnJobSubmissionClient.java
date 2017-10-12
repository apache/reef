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
package org.apache.reef.bridge.client;

import net.jcip.annotations.Immutable;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.driver.parameters.DriverIsUnmanaged;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.common.launch.parameters.DriverLaunchCommandPrefix;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.client.SecurityTokenProvider;
import org.apache.reef.runtime.yarn.client.YarnSubmissionHelper;
import org.apache.reef.runtime.yarn.client.unmanaged.YarnProxyUser;
import org.apache.reef.runtime.yarn.client.uploader.JobFolder;
import org.apache.reef.runtime.yarn.client.uploader.JobUploader;
import org.apache.reef.runtime.yarn.driver.parameters.FileSystemUrl;
import org.apache.reef.runtime.yarn.driver.parameters.JobSubmissionDirectoryPrefix;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.JARFileMaker;

import javax.inject.Inject;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Java-side of the C# YARN Job Submission API.
 */
@Immutable
public final class YarnJobSubmissionClient {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionClient.class.getName());

  private final boolean isUnmanaged;
  private final List<String> commandPrefixList;
  private final JobUploader uploader;
  private final REEFFileNames fileNames;
  private final YarnConfiguration yarnConfiguration;
  private final ClasspathProvider classpath;
  private final YarnProxyUser yarnProxyUser;
  private final SecurityTokenProvider tokenProvider;
  private final YarnSubmissionParametersFileGenerator jobSubmissionParametersGenerator;
  private final SecurityTokensReader securityTokensReader;
  private static final String DEFAULT_TOKEN_KIND = "NULL";

  @Inject
  private YarnJobSubmissionClient(
      @Parameter(DriverIsUnmanaged.class) final boolean isUnmanaged,
      @Parameter(DriverLaunchCommandPrefix.class) final List<String> commandPrefixList,
      final JobUploader uploader,
      final YarnConfiguration yarnConfiguration,
      final REEFFileNames fileNames,
      final ClasspathProvider classpath,
      final YarnProxyUser yarnProxyUser,
      final SecurityTokenProvider tokenProvider,
      final YarnSubmissionParametersFileGenerator jobSubmissionParametersGenerator,
      final SecurityTokensReader securityTokensReader) {

    this.isUnmanaged = isUnmanaged;
    this.commandPrefixList = commandPrefixList;
    this.uploader = uploader;
    this.fileNames = fileNames;
    this.yarnConfiguration = yarnConfiguration;
    this.classpath = classpath;
    this.yarnProxyUser = yarnProxyUser;
    this.tokenProvider = tokenProvider;
    this.jobSubmissionParametersGenerator = jobSubmissionParametersGenerator;
    this.securityTokensReader = securityTokensReader;
  }

  /**
   * @param driverFolder the folder containing the `reef` folder. Only that `reef` folder will be in the JAR.
   * @return the jar file
   * @throws IOException
   */
  private File makeJar(final File driverFolder) throws IOException {
    Validate.isTrue(driverFolder.exists());
    final File jarFile = new File(driverFolder.getParentFile(), driverFolder.getName() + ".jar");
    final File reefFolder = new File(driverFolder, fileNames.getREEFFolderName());
    if (!reefFolder.isDirectory()) {
      throw new FileNotFoundException(reefFolder.getAbsolutePath());
    }

    new JARFileMaker(jarFile).addChildren(reefFolder).close();
    return jarFile;
  }

  private void launch(final YarnClusterSubmissionFromCS yarnSubmission) throws IOException, YarnException {

    this.setupCredentials(yarnSubmission);

    // ------------------------------------------------------------------------
    // Get an application ID
    try (final YarnSubmissionHelper submissionHelper = new YarnSubmissionHelper(
        this.yarnConfiguration, this.fileNames, this.classpath, this.yarnProxyUser,
        this.tokenProvider, this.isUnmanaged, this.commandPrefixList)) {

      // ------------------------------------------------------------------------
      // Prepare the JAR
      final JobFolder jobFolderOnDFS = this.uploader.createJobFolder(submissionHelper.getApplicationId());
      this.jobSubmissionParametersGenerator.writeConfiguration(yarnSubmission, jobFolderOnDFS);
      final File jarFile = makeJar(yarnSubmission.getDriverFolder());
      LOG.log(Level.INFO, "Created job submission jar file: {0}", jarFile);

      // ------------------------------------------------------------------------
      // Upload the JAR
      LOG.info("Uploading job submission JAR");
      final LocalResource jarFileOnDFS = jobFolderOnDFS.uploadAsLocalResource(jarFile, LocalResourceType.ARCHIVE);
      LOG.info("Uploaded job submission JAR");

      // ------------------------------------------------------------------------
      // Upload the job file
      final LocalResource jobFileOnDFS = jobFolderOnDFS.uploadAsLocalResource(
          new File(yarnSubmission.getDriverFolder(), fileNames.getYarnBootstrapJobParamFilePath()),
          LocalResourceType.FILE);

      final List<String> confFiles = new ArrayList<>();
      confFiles.add(fileNames.getYarnBootstrapJobParamFilePath());
      confFiles.add(fileNames.getYarnBootstrapAppParamFilePath());

      // ------------------------------------------------------------------------
      // Submit
      submissionHelper
          .addLocalResource(this.fileNames.getREEFFolderName(), jarFileOnDFS)
          .addLocalResource(fileNames.getYarnBootstrapJobParamFilePath(), jobFileOnDFS)
          .setApplicationName(yarnSubmission.getJobId())
          .setDriverResources(yarnSubmission.getDriverMemory(), 1)
          .setPriority(yarnSubmission.getPriority())
          .setQueue(yarnSubmission.getQueue())
          .setMaxApplicationAttempts(yarnSubmission.getMaxApplicationSubmissions())
          .setPreserveEvaluators(yarnSubmission.getDriverRecoveryTimeout() > 0)
          .setLauncherClass(YarnBootstrapREEFLauncher.class)
          .setConfigurationFilePaths(confFiles)
          .setDriverStdoutPath(yarnSubmission.getYarnDriverStdoutFilePath())
          .setDriverStderrPath(yarnSubmission.getYarnDriverStderrFilePath())
          .submit();
      writeDriverHttpEndPoint(yarnSubmission.getDriverFolder(),
          submissionHelper.getStringApplicationId(), jobFolderOnDFS.getPath());
    }
  }

  private void setupCredentials(
      final YarnClusterSubmissionFromCS yarnSubmission) throws IOException {

    final File securityTokensFile = new File(fileNames.getSecurityTokensFile());

    if (securityTokensFile.exists()) {
      LOG.log(Level.INFO, "Writing security tokens to user credential");
      this.addTokensToCurrentUser();
    } else if (!yarnSubmission.getTokenKind().equalsIgnoreCase(DEFAULT_TOKEN_KIND)) {
      // To support backward compatibility
      LOG.log(Level.INFO, "Writing security token to user credential");
      this.writeSecurityTokenToUserCredential(yarnSubmission);
    } else {
      LOG.log(Level.FINE, "Did not find security token");
    }
  }

  private void writeSecurityTokenToUserCredential(
      final YarnClusterSubmissionFromCS yarnSubmission) throws IOException {

    final String securityTokenIdentifierFile = fileNames.getSecurityTokenIdentifierFile();
    final String securityTokenPasswordFile = fileNames.getSecurityTokenPasswordFile();
    final Text tokenKind = new Text(yarnSubmission.getTokenKind());
    final Text tokenService = new Text(yarnSubmission.getTokenService());
    final byte[] identifier = Files.readAllBytes(Paths.get(securityTokenIdentifierFile));
    final byte[] password = Files.readAllBytes(Paths.get(securityTokenPasswordFile));

    final Token<TokenIdentifier> token = new Token<>(identifier, password, tokenKind, tokenService);

    final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    currentUser.addToken(token);
  }

  private void addTokensToCurrentUser() throws IOException {
    final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    logToken(Level.INFO, "Before adding client tokens,", currentUser);
    this.securityTokensReader.addTokensFromFile(currentUser);
    // Log info for debug purpose for now until it is stable then we can change the log level to FINE.
    logToken(Level.INFO, "After adding client tokens,", currentUser);
  }

  /**
   * Log all the tokens in the container for thr user.
   *
   * @param logLevel - the log level.
   * @param msgPrefix - msg prefix for log.
   * @param user - the UserGroupInformation object.
   */
  private static void logToken(final Level logLevel, final String msgPrefix, final UserGroupInformation user) {
    if (LOG.isLoggable(logLevel)) {
      LOG.log(logLevel, "{0} number of tokens: [{1}].",
          new Object[] {msgPrefix, user.getCredentials().numberOfTokens()});
      for (final org.apache.hadoop.security.token.Token t : user.getCredentials().getAllTokens()) {
        LOG.log(logLevel, "Token service: {0}, token kind: {1}.", new Object[] {t.getService(), t.getKind()});
      }
    }
  }

  /**
   * We leave a file behind in job submission directory so that clr client can figure out
   * the applicationId and yarn rest endpoint.
   * @param driverFolder
   * @param applicationId
   * @throws IOException
   */
  private void writeDriverHttpEndPoint(final File driverFolder,
                                       final String applicationId,
                                       final Path dfsPath) throws  IOException {
    final FileSystem fs = FileSystem.get(yarnConfiguration);
    final Path httpEndpointPath = new Path(dfsPath, fileNames.getDriverHttpEndpoint());

    String trackingUri = null;
    LOG.log(Level.INFO, "Attempt to reading " + httpEndpointPath.toString());
    for (int i = 0; i < 60; i++) {
      try {
        LOG.log(Level.FINE, "Attempt " + i + " reading " + httpEndpointPath.toString());
        if (fs.exists(httpEndpointPath)) {
          final FSDataInputStream input = fs.open(httpEndpointPath);
          final BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
          trackingUri = reader.readLine();
          reader.close();
          break;
        }
      } catch (Exception ignored) {
        // readLine might throw IOException although httpEndpointPath file exists.
        // the for-loop waits until the actual content of file is written completely
      }
      try{
        Thread.sleep(1000);
      } catch(InterruptedException ex2) {
        break;
      }
    }

    if (null == trackingUri) {
      trackingUri = "";
      LOG.log(Level.WARNING, "Failed reading " + httpEndpointPath.toString());
    } else {
      LOG.log(Level.INFO, "Completed reading trackingUri :" + trackingUri);
    }

    final File driverHttpEndpointFile = new File(driverFolder, fileNames.getDriverHttpEndpoint());
    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                   new FileOutputStream(driverHttpEndpointFile), StandardCharsets.UTF_8));
    out.write(applicationId + "\n");
    out.write(trackingUri + "\n");
    String addr = yarnConfiguration.get("yarn.resourcemanager.webapp.address");
    if (null == addr || addr.startsWith("0.0.0.0")) {
      String str2 = yarnConfiguration.get("yarn.resourcemanager.ha.rm-ids");
      if (null != str2) {
        for (String rm : str2.split(",")) {
          out.write(yarnConfiguration.get("yarn.resourcemanager.webapp.address." + rm) +"\n");
        }
      }
    } else {
      out.write(addr +"\n");
    }
    out.close();
  }

  private static final Tang TANG = Tang.Factory.getTang();

  /**
   * .NET client calls into this main method for job submission.
   * For arguments detail:
   * @see YarnClusterSubmissionFromCS#fromJobSubmissionParametersFile(File, File)
   */
  public static void main(final String[] args) throws InjectionException, IOException, YarnException {
    final File jobSubmissionParametersFile = new File(args[0]);
    final File appSubmissionParametersFile = new File(args[1]);

    if (!(appSubmissionParametersFile.exists() && appSubmissionParametersFile.canRead())) {
      throw new IOException("Unable to open and read " + appSubmissionParametersFile.getAbsolutePath());
    }

    if (!(jobSubmissionParametersFile.exists() && jobSubmissionParametersFile.canRead())) {
      throw new IOException("Unable to open and read " + jobSubmissionParametersFile.getAbsolutePath());
    }

    final YarnClusterSubmissionFromCS yarnSubmission =
        YarnClusterSubmissionFromCS.fromJobSubmissionParametersFile(
            appSubmissionParametersFile, jobSubmissionParametersFile);

    LOG.log(Level.INFO, "YARN job submission received from C#: {0}", yarnSubmission);

    if (!yarnSubmission.getFileSystemUrl().equalsIgnoreCase(FileSystemUrl.DEFAULT_VALUE)) {
      LOG.log(Level.INFO, "getFileSystemUrl: {0}", yarnSubmission.getFileSystemUrl());
    } else {
      LOG.log(Level.INFO, "FileSystemUrl is not set, use default from the environment.");
    }

    final List<String> launchCommandPrefix = new ArrayList<String>() {{
        final REEFFileNames reefFileNames = TANG.newInjector().getInstance(REEFFileNames.class);
        add(reefFileNames.getDriverLauncherExeFile().getPath());
      }};

    final Configuration yarnJobSubmissionClientConfig = TANG.newConfigurationBuilder()
        .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
        .bindConstructor(org.apache.hadoop.yarn.conf.YarnConfiguration.class, YarnConfigurationConstructor.class)
        .bindNamedParameter(JobSubmissionDirectoryPrefix.class, yarnSubmission.getJobSubmissionDirectoryPrefix())
        .bindNamedParameter(FileSystemUrl.class, yarnSubmission.getFileSystemUrl())
        .bindList(DriverLaunchCommandPrefix.class, launchCommandPrefix)
        .build();

    final YarnJobSubmissionClient client =
        TANG.newInjector(yarnJobSubmissionClientConfig).getInstance(YarnJobSubmissionClient.class);

    client.launch(yarnSubmission);

    LOG.log(Level.INFO, "Returned from launch in Java YarnJobSubmissionClient");
    System.exit(0);
    LOG.log(Level.INFO, "End of main in Java YarnJobSubmissionClient");
  }
}
