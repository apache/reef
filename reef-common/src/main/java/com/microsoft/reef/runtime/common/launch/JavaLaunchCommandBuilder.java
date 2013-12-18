package com.microsoft.reef.runtime.common.launch;


import com.microsoft.reef.runtime.common.Launcher;
import com.microsoft.reef.runtime.common.utils.JavaUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public final class JavaLaunchCommandBuilder implements LaunchCommandBuilder {
  private String stderr_path = null;
  private String stdout_path = null;
  private String errorHandlerRID = null;
  private String launchID = null;
  private int megaBytes = 0;
  private String evaluatorConfigurationPath = null;
  private String javaPath = null;
  private String classPath = null;

  @Override
  public List<String> build() {
    return new ArrayList<String>() {{

      if (javaPath == null || javaPath.isEmpty()) {
        add(JavaUtils.getJavaBinary());
      } else {
        add(javaPath);
      }

      add("-XX:PermSize=128m");
      add("-XX:MaxPermSize=128m");
      // Set Xmx based on am memory size
      add("-Xmx" + megaBytes + "m");

      add("-classpath");
      add(classPath != null ? classPath : JavaUtils.getClasspath());

      // add("-Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000");

      Launcher.propagateProperties(this, Launcher.LOGGING_PROPERTIES);

      add(Launcher.class.getName());

      add("-" + Launcher.ERROR_HANDLER_RID);
      add(errorHandlerRID);
      add("-" + Launcher.LAUNCH_ID);
      add(launchID);
      add("-" + Launcher.EVALUATOR_CONFIGURATION_ARG);
      add(evaluatorConfigurationPath);

      if (stdout_path != null && !stdout_path.isEmpty()) {
        add("1>");
        add(stdout_path);
      }

      if (stderr_path != null && !stderr_path.isEmpty()) {
        add("2>");
        add(stderr_path);
      }

      final StringBuilder args = new StringBuilder();
      for (final String s : this) {
        args.append(s).append(' ');
      }
    }};
  }

  @Override
  public JavaLaunchCommandBuilder setErrorHandlerRID(final String errorHandlerRID) {
    this.errorHandlerRID = errorHandlerRID;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setLaunchID(final String launchID) {
    this.launchID = launchID;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setMemory(final int megaBytes) {
    this.megaBytes = megaBytes;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setConfigurationFileName(final String configurationFileName) {
    this.evaluatorConfigurationPath = configurationFileName;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setStandardOut(final String standardOut) {
    this.stdout_path = standardOut;
    return this;
  }

  @Override
  public JavaLaunchCommandBuilder setStandardErr(final String standardErr) {
    this.stderr_path = standardErr;
    return this;
  }

  /**
   * Set the path to the java executable. Will default to a heauristic search if not set.
   *
   * @param path
   * @return
   */
  public JavaLaunchCommandBuilder setJavaPath(final String path) {
    this.javaPath = path;
    return this;
  }

  public JavaLaunchCommandBuilder setClassPath(final String classPath) {
    this.classPath = classPath;
    return this;
  }

  public JavaLaunchCommandBuilder setClassPath(final List<String> classPathElements) {
    this.classPath = StringUtils.join(classPathElements, File.pathSeparatorChar);
    return this;
  }

}
