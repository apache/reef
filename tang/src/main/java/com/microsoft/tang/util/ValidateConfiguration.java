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

    final InputStream chin = new FileInputStream(ch);
    final ClassHierarchyProto.Node root;

    try {
      root = ClassHierarchyProto.Node.parseFrom(chin);
    } finally {
      chin.close();
    }
    final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
    final ConfigurationBuilder cb = t.newConfigurationBuilder(ch);
    
    ConfigurationFile.addConfiguration(cb, inConfig);
    if(target != null) {
      Injector i = t.newInjector(cb.build());
      InjectionPlan<?> ip = i.getInjectionPlan(target);
      if(!ip.isInjectable()) {
        throw new InjectionException(target + " is not injectable: " + ip.toCantInjectString());
      }
    }
    ConfigurationFile.writeConfigurationFile(cb.build(), outConfig);
//    Injector i = t.newInjector(cb.build());
//    InjectionPlan<?> ip = i.getInjectionPlan(target);
//    
//    final OutputStream ipout = new FileOutputStream(injectionPlan);
//    try {
//      new ProtocolBufferInjectionPlan().serialize(ip).writeTo(ipout);
//    } finally {
//      ipout.close();
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
