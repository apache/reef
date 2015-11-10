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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.driver.parameters.MaxApplicationSubmissions;
import org.apache.reef.driver.parameters.ResourceManagerPreserveEvaluators;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.launch.parameters.DriverLaunchCommandPrefix;
import org.apache.reef.runtime.yarn.client.SecurityTokenProvider;
import org.apache.reef.runtime.yarn.client.YarnSubmissionHelper;
import org.apache.reef.runtime.yarn.client.uploader.JobFolder;
import org.apache.reef.runtime.yarn.client.uploader.JobUploader;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.JARFileMaker;

import javax.inject.Inject;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Java-side of the C# YARN Job Submission API.
 */
@SuppressWarnings("checkstyle:hideutilityclassconstructor")
public final class YarnJobSubmissionClient {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionClient.class.getName());
  private final JobUploader uploader;
  private final ConfigurationSerializer configurationSerializer;
  private final REEFFileNames fileNames;
  private final YarnConfiguration yarnConfiguration;
  private final ClasspathProvider classpath;
  private final int maxApplicationSubmissions;
  private final SecurityTokenProvider tokenProvider;
  private final List<String> commandPrefixList;
  private final YarnDriverConfigurationGenerator configurationGenerator;

  @Inject
  YarnJobSubmissionClient(final JobUploader uploader,
                          final YarnConfiguration yarnConfiguration,
                          final ConfigurationSerializer configurationSerializer,
                          final REEFFileNames fileNames,
                          final ClasspathProvider classpath,
                          @Parameter(MaxApplicationSubmissions.class)
                          final int maxApplicationSubmissions,
                          @Parameter(DriverLaunchCommandPrefix.class)
                          final List<String> commandPrefixList,
                          final SecurityTokenProvider tokenProvider,
                          final YarnDriverConfigurationGenerator configurationGenerator) {
    this.uploader = uploader;
    this.configurationSerializer = configurationSerializer;
    this.fileNames = fileNames;
    this.yarnConfiguration = yarnConfiguration;
    this.classpath = classpath;
    this.maxApplicationSubmissions = maxApplicationSubmissions;
    this.tokenProvider = tokenProvider;
    this.commandPrefixList = commandPrefixList;
    this.configurationGenerator = configurationGenerator;
  }

  /**
   * @param driverFolder the folder containing the `reef` folder. Only that `reef` folder will be in the JAR.
   * @return the jar file
   * @throws IOException
   */
  private File makeJar(final File driverFolder) throws IOException {
    final File jarFile = new File(driverFolder.getParentFile(), driverFolder.getName() + ".jar");
    final File reefFolder = new File(driverFolder, fileNames.getREEFFolderName());
    if (!reefFolder.isDirectory()) {
      throw new FileNotFoundException(reefFolder.getAbsolutePath());
    }

    new JARFileMaker(jarFile).addChildren(reefFolder).close();
    return jarFile;
  }

  private void launch(final YarnSubmissionFromCS yarnSubmission) throws IOException, YarnException {
    // ------------------------------------------------------------------------
    // Get an application ID
    try (final YarnSubmissionHelper submissionHelper =
             new YarnSubmissionHelper(yarnConfiguration, fileNames, classpath, tokenProvider, commandPrefixList)) {

      // ------------------------------------------------------------------------
      // Prepare the JAR
      final JobFolder jobFolderOnDFS = this.uploader.createJobFolder(submissionHelper.getApplicationId());
      final Configuration jobSubmissionConfiguration =
          this.configurationGenerator.writeConfiguration(yarnSubmission.getDriverFolder(),
            yarnSubmission.getJobId(),
            jobFolderOnDFS.getPath().toString());
      final File jarFile = makeJar(yarnSubmission.getDriverFolder());
      LOG.log(Level.INFO, "Created job submission jar file: {0}", jarFile);

      // ------------------------------------------------------------------------
      // Upload the JAR
      LOG.info("Uploading job submission JAR");
      final LocalResource jarFileOnDFS = jobFolderOnDFS.uploadAsLocalResource(jarFile);
      LOG.info("Uploaded job submission JAR");

      final Injector jobParamsInjector = Tang.Factory.getTang().newInjector(jobSubmissionConfiguration);

      // ------------------------------------------------------------------------
      // Submit
      try {
        submissionHelper
            .addLocalResource(this.fileNames.getREEFFolderName(), jarFileOnDFS)
            .setApplicationName(yarnSubmission.getJobId())
            .setDriverMemory(yarnSubmission.getDriverMemory())
            .setPriority(yarnSubmission.getPriority())
            .setQueue(yarnSubmission.getQueue())
            .setMaxApplicationAttempts(this.maxApplicationSubmissions)
            .setPreserveEvaluators(jobParamsInjector.getNamedInstance(ResourceManagerPreserveEvaluators.class))
            .submit();
      } catch (InjectionException ie) {
        throw new RuntimeException("Unable to submit job due to " + ie);
      }
      writeDriverHttpEndPoint(yarnSubmission.getDriverFolder(),
          submissionHelper.getStringApplicationId(), jobFolderOnDFS.getPath());
    }
  }

  private static void writeSecurityTokenToUserCredential(final YarnSubmissionFromCS yarnSubmission) throws IOException {
    final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    final REEFFileNames fileNames = new REEFFileNames();
    final String securityTokenIdentifierFile = fileNames.getSecurityTokenIdentifierFile();
    final String securityTokenPasswordFile = fileNames.getSecurityTokenPasswordFile();
    final Text tokenKind = new Text(yarnSubmission.getTokenKind());
    final Text tokenService = new Text(yarnSubmission.getTokenService());
    byte[] identifier = Files.readAllBytes(Paths.get(securityTokenIdentifierFile));
    byte[] password = Files.readAllBytes(Paths.get(securityTokenPasswordFile));
    Token token = new Token(identifier, password, tokenKind, tokenService);
    currentUser.addToken(token);
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
      } catch (Exception ex) {
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

  /**
   * .NET client calls into this main method for job submission.
   * For arguments detail:
   * @see org.apache.reef.bridge.client.YarnSubmissionFromCS#fromBootstrapConfigFile(String)
   */
  public static void main(final String[] args) throws InjectionException, IOException, YarnException {
    final YarnSubmissionFromCS yarnSubmission = YarnSubmissionFromCS.fromBootstrapConfigFile(args[0]);
    LOG.log(Level.INFO, "YARN job submission received from C#: {0}", yarnSubmission);
    if (!yarnSubmission.getTokenKind().equalsIgnoreCase("NULL")) {
      // We have to write security token to user credential before YarnJobSubmissionClient is created
      // as that will need initialization of FileSystem which could need the token.
      LOG.log(Level.INFO, "Writing security token to user credential");
      writeSecurityTokenToUserCredential(yarnSubmission);
    } else{
      LOG.log(Level.FINE, "Did not find security token");
    }

    final Configuration yarnConfiguration = yarnSubmission.getRuntimeConfiguration();
    final YarnJobSubmissionClient client = Tang.Factory.getTang()
        .newInjector(yarnConfiguration)
        .getInstance(YarnJobSubmissionClient.class);
    client.launch(yarnSubmission);
    LOG.log(Level.INFO, "Returned from launch in Java YarnJobSubmissionClient");
    System.exit(0);
    LOG.log(Level.INFO, "End of main in Java YarnJobSubmissionClient");
  }
}
