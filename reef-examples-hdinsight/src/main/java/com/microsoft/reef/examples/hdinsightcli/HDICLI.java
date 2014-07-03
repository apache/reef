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
package com.microsoft.reef.examples.hdinsightcli;

import com.microsoft.reef.runtime.hdinsight.client.UnsafeHDInsightRuntimeConfiguration;
import com.microsoft.reef.runtime.hdinsight.client.yarnrest.ApplicationID;
import com.microsoft.reef.runtime.hdinsight.client.yarnrest.ApplicationState;
import com.microsoft.reef.runtime.hdinsight.client.yarnrest.HDInsightInstance;
import com.microsoft.tang.Tang;
import org.apache.commons.cli.*;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class for the HDInsight REST commandline
 */
public final class HDICLI {
  private static final Logger LOG = Logger.getLogger(HDICLI.class.getName());
  private static final String KILL = "kill";
  private static final String LOGS = "logs";
  private static final String LIST = "list";

  private final HDInsightInstance hdInsightInstance;
  private final Options options;
  private final LogFetcher logFetcher;

  @Inject
  public HDICLI(final HDInsightInstance hdInsightInstance,
                final LogFetcher logFetcher) {
    this.hdInsightInstance = hdInsightInstance;
    this.logFetcher = logFetcher;
    final OptionGroup commands = new OptionGroup()
        .addOption(OptionBuilder.withArgName(KILL).hasArg().withDescription("Kills the given application").create(KILL))
        .addOption(OptionBuilder.withArgName(LOGS).hasArg().withDescription("Kills the given application").create(LOGS))
        .addOption(OptionBuilder.withArgName(LIST).withDescription("Kills the given application").create(LIST));
    this.options = new Options().addOptionGroup(commands);
  }

  public void run(final String[] args) throws Exception {
    final CommandLineParser parser = new PosixParser();

    final CommandLine line = parser.parse(options, args);
    if (line.hasOption(KILL)) {
      this.kill(line.getOptionValue(KILL));
    } else if (line.hasOption(LOGS)) {
      this.logs(line.getOptionValue(LOGS));
    } else if (line.hasOption(LIST)) {
      this.list();
    } else {
      throw new Exception("Unable to parse command line");
    }

  }

  private void kill(final String applicationId) {
    LOG.log(Level.INFO, "Killing application [{0}]", applicationId);
    this.hdInsightInstance.killApplication(applicationId);
  }

  private void logs(final String applicationId) throws IOException {
    LOG.log(Level.INFO, "Fetching logs for application [{0}]", applicationId);
    this.logFetcher.fetch(applicationId, new OutputStreamWriter(System.out));
  }

  private void list() throws IOException {
    LOG.log(Level.INFO, "Listing applications");
    final ApplicationID applicationID = this.hdInsightInstance.getApplicationID();
    System.out.println(applicationID);
    final List<ApplicationState> applications = this.hdInsightInstance.listApplications();
    for (final ApplicationState appState : applications) {
      if (appState.getState().equals("RUNNING")) {
        System.out.println(appState.getId() + "\t" + appState.getName());
      }
    }
  }


  public static void main(final String[] args) throws Exception {
    Tang.Factory.getTang()
        .newInjector(UnsafeHDInsightRuntimeConfiguration.fromEnvironment())
        .getInstance(HDICLI.class).run(args);
  }

}
