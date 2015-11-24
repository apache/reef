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

import org.apache.commons.lang.Validate;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.reef.runtime.common.files.RuntimeClasspathProvider;
import org.apache.reef.runtime.yarn.YarnClasspathProvider;
import org.apache.reef.runtime.yarn.client.uploader.JobUploader;
import org.apache.reef.runtime.yarn.util.YarnConfigurationConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class that uploads job resource to HDFS.
 */
public final class JobResourceUploader {
  private static final Logger LOG = Logger.getLogger(JobResourceUploader.class.getName());

  private JobResourceUploader(){}

  /**
   * This class is invoked from Org.Apache.REEF.Client.Yarn.LegacyJobResourceUploader in .NET code.
   * Arguments:
   * [0] : Local path for already generated archive
   * [1] : Path of job submission directory
   * [2] : File path for output with details of uploaded resource
   */
  public static void main(final String[] args) throws InjectionException, IOException {
    Validate.isTrue(args.length == 3, "Job resource uploader requires 3 args");
    final File localFile = new File(args[0]);
    Validate.isTrue(localFile.exists(), "Local archive does not exist " + localFile.getAbsolutePath());
    final String jobSubmissionDirectory = args[1];
    final String localOutputPath = args[2];

    LOG.log(Level.INFO, "Received args: LocalPath " + localFile.getAbsolutePath() + " Submission directory " +
        jobSubmissionDirectory + " LocalOutputPath " + localOutputPath);
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(RuntimeClasspathProvider.class, YarnClasspathProvider.class)
        .bindConstructor(org.apache.hadoop.yarn.conf.YarnConfiguration.class, YarnConfigurationConstructor.class)
        .build();

    final JobUploader jobUploader = Tang.Factory.getTang()
        .newInjector(configuration)
        .getInstance(JobUploader.class);
    final LocalResource localResource = jobUploader.createJobFolder(jobSubmissionDirectory)
        .uploadAsLocalResource(localFile);

    // Output: <UploadedPath>;<LastModificationUnixTimestamp>;<ResourceSize>
    final URL resource = localResource.getResource();
    final String outputString = String.format("%s://%s:%d%s;%d;%d", resource.getScheme(), resource.getHost(),
        resource.getPort(), resource.getFile(), localResource.getTimestamp(), localResource.getSize());
    LOG.log(Level.INFO, "Writing output: " + outputString);
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(localOutputPath), "utf-8"))) {
      writer.write(outputString);
    }

    LOG.log(Level.FINER, "Done writing output file");
  }
}
