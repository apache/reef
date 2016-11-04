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
package org.apache.reef.io.checkpoint.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.io.checkpoint.CheckpointID;
import org.apache.reef.io.checkpoint.CheckpointNamingService;
import org.apache.reef.io.checkpoint.CheckpointService;
import org.apache.reef.io.checkpoint.RandomNameCNS;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

import javax.inject.Inject;
import java.io.IOException;

/**
 * ConfigurationModule for the FSCheckpointService.
 * This can be used to create Evaluator-side configurations of the checkpointing service.
 */
@DriverSide
@Public
public class FSCheckPointServiceConfiguration extends ConfigurationModuleBuilder {

  /**
   * Use local file system if true; otherwise, use HDFS.
   */
  public static final RequiredParameter<Boolean> IS_LOCAL = new RequiredParameter<>();

  /**
   * Path to be used to store the checkpoints on file system.
   */
  public static final RequiredParameter<String> PATH = new RequiredParameter<>();

  /**
   * Replication factor to be used for the checkpoints.
   */
  public static final OptionalParameter<Short> REPLICATION_FACTOR = new OptionalParameter<>();

  /**
   * Prefix for checkpoint files (optional).
   */
  public static final OptionalParameter<String> PREFIX = new OptionalParameter<>();


  public static final ConfigurationModule CONF = new FSCheckPointServiceConfiguration()

      .bindImplementation(CheckpointService.class, FSCheckpointService.class) // Use the HDFS based checkpoints
      .bindImplementation(CheckpointNamingService.class, RandomNameCNS.class) // Use Random Names for the checkpoints
      .bindImplementation(CheckpointID.class, FSCheckpointID.class)
      .bindConstructor(FileSystem.class, FileSystemConstructor.class)

      .bindNamedParameter(FileSystemConstructor.IsLocal.class, IS_LOCAL)
      .bindNamedParameter(FSCheckpointService.PATH.class, PATH)
      .bindNamedParameter(FSCheckpointService.ReplicationFactor.class, REPLICATION_FACTOR)
      .bindNamedParameter(RandomNameCNS.PREFIX.class, PREFIX)
      .build();

  /**
   * Constructor for Hadoop FileSystem instances.
   * This assumes that Hadoop Configuration is in the CLASSPATH.
   */
  public static class FileSystemConstructor implements ExternalConstructor<FileSystem> {

    /**
     * If false, use default values for Hadoop configuration; otherwise, load from config file.
     * Set to false when REEF is running in local mode.
     */
    private final boolean loadConfig;

    @Inject
    public FileSystemConstructor(@Parameter(IsLocal.class) final boolean isLocal) {
      this.loadConfig = !isLocal;
    }

    @Override
    public FileSystem newInstance() {
      try {
        return FileSystem.get(new Configuration(this.loadConfig));
      } catch (final IOException ex) {
        throw new RuntimeException("Unable to create a FileSystem instance." +
            " Probably Hadoop configuration is not in the CLASSPATH", ex);
      }
    }

    @NamedParameter(doc = "Use local file system if true; otherwise, use HDFS.")
    static class IsLocal implements Name<Boolean> {
    }
  }
}
