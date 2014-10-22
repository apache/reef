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
package com.microsoft.tang.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.inject.Inject;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferInjectionPlan;
import com.microsoft.tang.proto.ClassHierarchyProto;

public class ValidateConfiguration {
  @NamedParameter(short_name="class")
  public class Target implements Name<String> { }
  @NamedParameter(short_name="ch")
  public class ClassHierarchyIn implements Name<File> { }
  @NamedParameter(short_name="in")
  public class ConfigurationIn implements Name<File> { }
  @NamedParameter(short_name="out")
  public class ConfigurationOut implements Name<File> { }
//  @NamedParameter(short_name="ip")
//  public class InjectionPlanOut implements Name<File> { }
  
  public static class FileParser implements ExternalConstructor<File> {
    private final File f;
    @Inject
    FileParser(String name) {
      f = new File(name);
    }
    @Override
    public File newInstance() {
      return f;
    }
    
  }
  private final String target;
  private final File ch;
  private final File inConfig;
  private final File outConfig;
//  private final File injectionPlan;
  
  @Inject
  public ValidateConfiguration(
      @Parameter(ClassHierarchyIn.class) File ch,
      @Parameter(ConfigurationIn.class) File inConfig,
      @Parameter(ConfigurationOut.class) File outConfig)
  {
    this.target = null;
    this.ch = ch;
    this.inConfig = inConfig;
    this.outConfig = outConfig;
//    this.injectionPlan = injectionPlan;
  }
  @Inject
  public ValidateConfiguration(
      @Parameter(Target.class) String injectedClass,
      @Parameter(ClassHierarchyIn.class) File ch,
      @Parameter(ConfigurationIn.class) File inConfig,
      @Parameter(ConfigurationOut.class) File outConfig)
  {
    this.target = injectedClass;
    this.ch = ch;
    this.inConfig = inConfig;
    this.outConfig = outConfig;
  }

  public void validatePlan() throws IOException, BindException, InjectionException {

    final Tang t = Tang.Factory.getTang();

    final ClassHierarchyProto.Node root;
    try (final InputStream chin = new FileInputStream(this.ch)) {
      root = ClassHierarchyProto.Node.parseFrom(chin);
    }

    final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
    final ConfigurationBuilder cb = t.newConfigurationBuilder(ch);

    if(!inConfig.canRead()) {
      throw new IOException("Cannot read input config file: " + inConfig);
    }

    ConfigurationFile.addConfiguration(cb, inConfig);

    if (target != null) {
      final Injector i = t.newInjector(cb.build());
      final InjectionPlan<?> ip = i.getInjectionPlan(target);
      if (!ip.isInjectable()) {
        throw new InjectionException(target + " is not injectable: " + ip.toCantInjectString());
      }
    }

    ConfigurationFile.writeConfigurationFile(cb.build(), outConfig);

//    Injector i = t.newInjector(cb.build());
//    InjectionPlan<?> ip = i.getInjectionPlan(target);
//    try (final OutputStream ipout = new FileOutputStream(injectionPlan)) {
//      new ProtocolBufferInjectionPlan().serialize(ip).writeTo(ipout);
//    }
  }

  public static void main(String[] argv) throws IOException, BindException, InjectionException {
    @SuppressWarnings("unchecked")
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder((Class<? extends ExternalConstructor<?>>[])new Class[] { FileParser.class } );
    CommandLine cl = new CommandLine(cb);
    cl.processCommandLine(argv,
        Target.class,
        ClassHierarchyIn.class,
        ConfigurationIn.class,
        ConfigurationOut.class);
    ValidateConfiguration bip = Tang.Factory.getTang().newInjector(cb.build()).getInstance(ValidateConfiguration.class);
    bip.validatePlan();
  }
}
