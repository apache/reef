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
package com.microsoft.reef.io.checkpoint.fs;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.io.checkpoint.CheckpointID;
import com.microsoft.reef.io.checkpoint.CheckpointNamingService;
import com.microsoft.reef.io.checkpoint.CheckpointService;
import com.microsoft.reef.io.checkpoint.RandomNameCNS;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.RequiredParameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import javax.inject.Inject;
import java.io.IOException;

/**
 * ConfigurationModule for the FSCheckPointService.
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

  /**
   * Constructor for Hadoop FileSystem instances.
   * This assumes that Hadoop Configuration is in the CLASSPATH.
   */
  public static class FileSystemConstructor implements ExternalConstructor<FileSystem> {

    @NamedParameter(doc = "Use local file system if true; otherwise, use HDFS.")
    static class IS_LOCAL implements Name<Boolean> {
    }

    /**
     * If false, use default values for Hadoop configuration; otherwise, load from config file.
     * Set to false when REEF is running in local mode.
     */
    private final boolean loadConfig;

    @Inject
    public FileSystemConstructor(final @Parameter(IS_LOCAL.class) boolean isLocal) {
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
  }

  public static final ConfigurationModule CONF = new FSCheckPointServiceConfiguration()
      .bindImplementation(CheckpointService.class, FSCheckpointService.class) // Use the HDFS based ccheckpoints
      .bindImplementation(CheckpointNamingService.class, RandomNameCNS.class) // Use Random Names for the checkpoints
      .bindImplementation(CheckpointID.class, FSCheckpointID.class)
      .bindConstructor(FileSystem.class, FileSystemConstructor.class)
      .bindNamedParameter(FileSystemConstructor.IS_LOCAL.class, IS_LOCAL)
      .bindNamedParameter(FSCheckpointService.PATH.class, PATH)
      .bindNamedParameter(FSCheckpointService.REPLICATION_FACTOR.class, REPLICATION_FACTOR)
      .bindNamedParameter(RandomNameCNS.PREFIX.class, PREFIX)
      .build();
}
