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
package com.microsoft.reef.runtime.yarn.util;

import com.microsoft.reef.annotations.audience.Private;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;

@Private
public final class YarnUtils {

  public static final StringBuilder getClassPathBuilder(final YarnConfiguration yarnConfiguration) {
    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    final StringBuilder classPathBuilder = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$())
        .append(File.pathSeparatorChar).append("./*");
    for (final String c : yarnConfiguration.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathBuilder.append(File.pathSeparatorChar);
      classPathBuilder.append(c.trim());
    }
    classPathBuilder.append(File.pathSeparatorChar).append("./log4j.properties");

    // add the runtime classpath needed for tests to work
    if (yarnConfiguration.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathBuilder.append(File.pathSeparatorChar);
      classPathBuilder.append(System.getProperty("java.class.path"));
    }

    // ? env.put("CLASSPATH", classPathEnv.toString());
    return classPathBuilder;
  }

  public final static LocalResource getLocalResource(final FileSystem fs, final Path src, final Path dst) throws IOException {
    if (fs == null || fs.getUri() == null || fs.getUri().getAuthority() == null) {
      throw new RuntimeException("Unknown file system: " + fs);
    } else if (fs.getUri().getAuthority().equals(src.toUri().getAuthority())) {
      final FileContext fileContext = FileContext.getFileContext(fs.getUri());
      fileContext.createSymlink(src, dst, true);
    } else {
      // Assume it's on the local file system and we need to move it to HDFS
      fs.copyFromLocalFile(false, true, src, dst);
    }
    return getLocalResource(fs, dst);
  }

  public final static LocalResource getLocalResource(final FileSystem fs, final Path path) throws IOException {
    final FileStatus status = FileContext.getFileContext(fs.getUri()).getFileStatus(path);
    final LocalResource file = Records.newRecord(LocalResource.class);

    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need jar files to be untarred for now
    file.setType(LocalResourceType.FILE);
    // Set visibility of the resource
    // Setting to most private option
    file.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    file.setResource(ConverterUtils.getYarnUrlFromPath(status.getPath()));
    // Set timestamp and length of file so that the framework
    // can do basic sanity checks for the local resource
    // after it has been copied over to ensure it is the same
    // resource the client intended to use with the application
    file.setTimestamp(status.getModificationTime());
    file.setSize(status.getLen());
    return file;
  }


  public static final ContainerLaunchContext getContainerLaunchContext(final String command,
                                                                       final Map<String, LocalResource> localResources)
      throws IOException, URISyntaxException {

    // Set up the container launch context for the application master
    final ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);

    context.setLocalResources(localResources);

    // Set the env variables to be setup in the env where the application master will be run
    // Map<String, String> env = new HashMap<String, String>();
    // env.put("CLASSPATH", getYarnClasspath(conf));
    // context.setEnvironment(env);

    ////////////////////////////////////////////////////////////////////////

    context.setCommands(Arrays.asList(command));

    // For launching an AM Container, setting user here is not needed
    // Set user in ApplicationSubmissionContext
    // amContainer.setUser(amUser);

    return context;
  }
}
