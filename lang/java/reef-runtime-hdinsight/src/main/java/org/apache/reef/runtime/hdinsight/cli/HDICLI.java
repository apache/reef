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
package org.apache.reef.runtime.hdinsight.cli;

import org.apache.commons.cli.*;
import org.apache.reef.runtime.hdinsight.client.UnsafeHDInsightRuntimeConfiguration;
import org.apache.reef.runtime.hdinsight.client.yarnrest.ApplicationState;
import org.apache.reef.runtime.hdinsight.client.yarnrest.HDInsightInstance;
import org.apache.reef.tang.Tang;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class for the HDInsight REST commandline.
 */
public final class HDICLI {
  private static final Logger LOG = Logger.getLogger(HDICLI.class.getName());
  private static final String KILL = "kill";
  private static final String LOGS = "logs";
  private static final String LIST = "list";
  private static final String STATUS = "status";

  private final HDInsightInstance hdInsightInstance;
  private final Options options;
  private final LogFetcher logFetcher;

  @Inject
  HDICLI(final HDInsightInstance hdInsightInstance,
         final LogFetcher logFetcher) {
    this.hdInsightInstance = hdInsightInstance;
    this.logFetcher = logFetcher;
    final OptionGroup commands = new OptionGroup()
        .addOption(OptionBuilder.withArgName(KILL).hasArg()
            .withDescription("Kills the given application.").create(KILL))
        .addOption(OptionBuilder.withArgName(LOGS).hasArg()
            .withDescription("Fetches the logs for the given application.").create(LOGS))
        .addOption(OptionBuilder.withArgName(STATUS).hasArg()
            .withDescription("Fetches the status for the given application.").create(STATUS))
        .addOption(OptionBuilder.withArgName(LIST)
            .withDescription("Lists the application on the cluster.").create(LIST));
    this.options = new Options().addOptionGroup(commands);
  }

  /**
   * Helper method to setup apache commons logging.
   */
  private static void setupLogging() {
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.Jdk14Logger");
    System.setProperty(".level", "INFO");
  }

  public static void main(final String[] args) throws Exception {
    setupLogging();
    Tang.Factory.getTang()
        .newInjector(UnsafeHDInsightRuntimeConfiguration.fromEnvironment())
        .getInstance(HDICLI.class).run(args);
  }

  public void run(final String[] args) throws Exception {
    final CommandLineParser parser = new PosixParser();

    final CommandLine line = parser.parse(options, args);
    final List<String> positionalArguments = line.getArgList();
    if (line.hasOption(KILL)) {
      this.kill(line.getOptionValue(KILL));
    } else if (line.hasOption(LOGS)) {
      final String applicationId = line.getOptionValue(LOGS);
      if (positionalArguments.isEmpty()) {
        this.logs(applicationId);
      } else {
        this.logs(applicationId, new File(positionalArguments.get(0)));
      }
    } else if (line.hasOption(LIST)) {
      this.list();
    } else if (line.hasOption(STATUS)) {
      this.status(line.getOptionValue(STATUS));
    } else {
      throw new Exception("Unable to parse command line");
    }

  }

  /**
   * Kills the application with the given id.
   *
   * @param applicationId
   * @throws IOException
   */
  private void kill(final String applicationId) throws IOException {
    LOG.log(Level.INFO, "Killing application [{0}]", applicationId);
    this.hdInsightInstance.killApplication(applicationId);
  }

  /**
   * Fetches the logs for the application with the given id and prints them to System.out.
   *
   * @param applicationId
   * @throws IOException
   */
  private void logs(final String applicationId) throws IOException {
    LOG.log(Level.INFO, "Fetching logs for application [{0}]", applicationId);
    this.logFetcher.fetch(applicationId, new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
  }

  /**
   * Fetches the logs for the application with the given id and stores them in the given folder. One file per container.
   *
   * @param applicationId
   * @param folder
   * @throws IOException
   */
  private void logs(final String applicationId, final File folder) throws IOException {
    LOG.log(Level.FINE, "Fetching logs for application [{0}] and storing them in folder [{1}]",
        new Object[]{applicationId, folder.getAbsolutePath()});
    if (!folder.exists() && !folder.mkdirs()) {
      LOG.log(Level.WARNING, "Failed to create [{0}]", folder.getAbsolutePath());
    }
    this.logFetcher.fetch(applicationId, folder);
  }

  /**
   * Fetches a list of all running applications.
   *
   * @throws IOException
   */
  private void list() throws IOException {
    LOG.log(Level.FINE, "Listing applications");
    final List<ApplicationState> applications = this.hdInsightInstance.listApplications();
    for (final ApplicationState appState : applications) {
      if (appState.getState().equals("RUNNING")) {
        System.out.println(appState.getId() + "\t" + appState.getName());
      }
    }
  }

  private void status(final String applicationId) throws IOException {
    final List<ApplicationState> applications = this.hdInsightInstance.listApplications();
    ApplicationState applicationState = null;
    for (final ApplicationState appState : applications) {
      if (appState.getId().equals(applicationId)) {
        applicationState = appState;
        break;
      }
    }

    if (applicationState == null) {
      throw new IOException("Unknown application: " + applicationId);
    }
    final ObjectMapper objectMapper = new ObjectMapper();
    final String status = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(applicationState);

    System.out.println(status);


  }

}
