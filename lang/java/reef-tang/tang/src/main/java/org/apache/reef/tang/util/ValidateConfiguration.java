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
package org.apache.reef.tang.util;

import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.implementation.InjectionPlan;
import org.apache.reef.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import org.apache.reef.tang.proto.ClassHierarchyProto;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ValidateConfiguration {
  private final String target;
  private final File ch;
  private final File inConfig;
  private final File outConfig;

  @Inject
  public ValidateConfiguration(
      @Parameter(ClassHierarchyIn.class) final File ch,
      @Parameter(ConfigurationIn.class) final File inConfig,
      @Parameter(ConfigurationOut.class) final File outConfig) {
    this.target = null;
    this.ch = ch;
    this.inConfig = inConfig;
    this.outConfig = outConfig;
  }

  @Inject
  public ValidateConfiguration(
      @Parameter(Target.class) final String injectedClass,
      @Parameter(ClassHierarchyIn.class) final File ch,
      @Parameter(ConfigurationIn.class) final File inConfig,
      @Parameter(ConfigurationOut.class) final File outConfig) {
    this.target = injectedClass;
    this.ch = ch;
    this.inConfig = inConfig;
    this.outConfig = outConfig;
  }

  public static void main(final String[] argv) throws IOException, BindException, InjectionException {
    @SuppressWarnings("unchecked") final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(
        (Class<? extends ExternalConstructor<?>>[]) new Class[]{FileParser.class});
    final CommandLine cl = new CommandLine(cb);
    cl.processCommandLine(argv,
        Target.class,
        ClassHierarchyIn.class,
        ConfigurationIn.class,
        ConfigurationOut.class);
    final ValidateConfiguration bip =
        Tang.Factory.getTang().newInjector(cb.build()).getInstance(ValidateConfiguration.class);
    bip.validatePlan();
  }

  public void validatePlan() throws IOException, BindException, InjectionException {

    final Tang t = Tang.Factory.getTang();

    // TODO[JIRA REEF-400] Use the AvroClassHierarchySerializer
    final ClassHierarchyProto.Node root;
    try (InputStream chin = new FileInputStream(this.ch)) {
      root = ClassHierarchyProto.Node.parseFrom(chin);
    }

    final ClassHierarchy classHierarchy = new ProtocolBufferClassHierarchy(root);

    if (!inConfig.canRead()) {
      throw new IOException("Cannot read input config file: " + inConfig);
    }

    final AvroConfigurationSerializer avroSerializer = new AvroConfigurationSerializer();
    final Configuration conf = avroSerializer.fromFile(inConfig, classHierarchy);

    if (target != null) {
      final Injector i = t.newInjector(conf);
      final InjectionPlan<?> ip = i.getInjectionPlan(target);
      if (!ip.isInjectable()) {
        throw new InjectionException(target + " is not injectable: " + ip.toCantInjectString());
      }
    }

    avroSerializer.toFile(conf, outConfig);
  }

  public static class FileParser implements ExternalConstructor<File> {
    private final File f;

    @Inject
    FileParser(final String name) {
      f = new File(name);
    }

    @Override
    public File newInstance() {
      return f;
    }

  }

  @NamedParameter(short_name = "class")
  public class Target implements Name<String> {
  }

  @NamedParameter(short_name = "ch")
  public class ClassHierarchyIn implements Name<File> {
  }

  @NamedParameter(short_name = "in")
  public class ConfigurationIn implements Name<File> {
  }

  @NamedParameter(short_name = "out")
  public class ConfigurationOut implements Name<File> {
  }
}
