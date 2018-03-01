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
package org.apache.reef.runtime.azbatch.evaluator;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.azbatch.parameters.EvaluatorShimConfigFilePath;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main entry point into the {@link EvaluatorShim}.
 */
@Private
@EvaluatorSide
public final class EvaluatorShimLauncher {

  private static final Logger LOG = Logger.getLogger(EvaluatorShimLauncher.class.getName());

  private final String configurationFilePath;
  private final ConfigurationSerializer configurationSerializer;

  @Inject
  EvaluatorShimLauncher(
      @Parameter(EvaluatorShimConfigFilePath.class) final String configurationFilePath,
      final ConfigurationSerializer configurationSerializer) {
    this.configurationFilePath = configurationFilePath;
    this.configurationSerializer = configurationSerializer;
  }

  /**
   * Launch the {@link EvaluatorShim}.
   * @throws Exception
   */
  public void launch() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector(readConfigurationFromDisk(this.configurationFilePath));
    final EvaluatorShim evaluatorShim = injector.getInstance(EvaluatorShim.class);
    evaluatorShim.run();
  }

  private Configuration readConfigurationFromDisk(final String configPath) {

    LOG.log(Level.FINER, "Loading configuration file: {0}", configPath);

    final File shimConfigurationFile = new File(configPath);

    if (!shimConfigurationFile.exists() || !shimConfigurationFile.canRead()) {
      throw new RuntimeException(
          "Configuration file " + configPath + " doesn't exist or is not readable.",
          new IOException(configPath));
    }

    try {
      final Configuration config = this.configurationSerializer.fromFile(shimConfigurationFile);
      LOG.log(Level.FINEST, "Configuration file loaded: {0}", configPath);
      return config;
    } catch (final IOException e) {
      throw new RuntimeException("Unable to parse the configuration file: " + configPath, e);
    }
  }

  private static Configuration parseCommandLine(final String[] args) {
    if (args.length != 1) {
      throw new RuntimeException("Expected configuration file name as an argument.");
    }

    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(EvaluatorShimConfigFilePath.class, args[0]);

    return confBuilder.build();
  }

  /**
   * The starting point of the evaluator shim launcher.
   */
  public static void main(final String[] args) throws Exception {
    LOG.log(Level.INFO, "Entering EvaluatorShimLauncher.main().");

    final Injector injector = Tang.Factory.getTang().newInjector(parseCommandLine(args));
    final EvaluatorShimLauncher launcher = injector.getInstance(EvaluatorShimLauncher.class);
    launcher.launch();
  }
}
