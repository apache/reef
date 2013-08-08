package com.microsoft.tang.formats;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.ConfigurationBuilderImpl;
import com.microsoft.tang.implementation.ConfigurationImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.ReflectionUtilities;

public class ConfigurationFile {
  /**
   * Writes this Configuration to the given File
   * 
   * @throws IOException
   * 
   */
  public static void writeConfigurationFile(Configuration c, File f)
      throws IOException {
    OutputStream o = new FileOutputStream(f);
    writeConfigurationFile(c, o);
    o.close();
  }

  /**
   * Writes a Configuration to the given OutputStream.
   * 
   * @param s
   * @throws IOException
   */
  @Deprecated
  public static void writeConfigurationFile(Configuration c, OutputStream o) {
    PrintStream p = new PrintStream(o);
    p.print(toConfigurationString(c));
    p.flush();
  }

  public static void addConfiguration(ConfigurationBuilder conf, File file)
      throws IOException, BindException {
    PropertiesConfiguration confFile;
    try {
      confFile = new PropertiesConfiguration(file);
    } catch (ConfigurationException e) {
      throw new BindException("Problem parsing config file", e);
    }
    processConfigFile(conf, confFile);
  }

  /**
   * 
   * @param conf
   *          This configuration builder will be modified to incorporate the
   *          contents of the configuration file.
   * @param s
   *          A string containing the contents of the configuration file.
   * @throws BindException
   */
  public static void addConfiguration(ConfigurationBuilder conf, String s)
      throws BindException {
    try {
      File tmp = File.createTempFile("tang", "tmp");
      FileOutputStream fos = new FileOutputStream(tmp);
      fos.write(s.getBytes());
      fos.close();
      addConfiguration(conf, tmp);
      tmp.delete();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void processConfigFile(ConfigurationBuilder conf,
      PropertiesConfiguration confFile) throws IOException, BindException {
    ConfigurationBuilderImpl ci = (ConfigurationBuilderImpl) conf;
    Iterator<String> it = confFile.getKeys();
    Map<String, String> importedNames = new HashMap<>();

    while (it.hasNext()) {
      String key = it.next();
      String longName = importedNames.get(key);
      String[] values = confFile.getStringArray(key);
      if (longName != null) {
        // System.err.println("Mapped " + key + " to " + longName);
        key = longName;
      }
      for (String value : values) {
        if (key.equals(ConfigurationBuilderImpl.IMPORT)) {
          ci.getClassHierarchy().getNode(value);
          final String[] tok = value.split(ReflectionUtilities.regexp);
          final String lastTok = tok[tok.length - 1];
          try {
            // ci.namespace.getNode(lastTok);
            ci.getClassHierarchy().getNode(lastTok);
            throw new IllegalArgumentException("Conflict on short name: " + lastTok);
          } catch (BindException e) {
            String oldValue = importedNames.put(lastTok, value);
            if (oldValue != null) {
              throw new IllegalArgumentException("Name conflict: "
                  + lastTok + " maps to " + oldValue + " and " + value);
            }
          }
        } else if (value.startsWith(ConfigurationBuilderImpl.INIT)) {
          String parseValue = value.substring(
              ConfigurationBuilderImpl.INIT.length(), value.length());
          parseValue = parseValue.replaceAll("^[\\s\\(]+", "");
          parseValue = parseValue.replaceAll("[\\s\\)]+$", "");
          String[] classes = parseValue.split("[\\s\\-]+");
          ci.registerLegacyConstructor(key, classes);
        } else {
          ci.bind(key, value);
        }
      }
    }
  }

  /**
   * Replace any \'s in the input string with \\. and any "'s with \".
   * @param in
   * @return
   */
  private static String escape(String in) {
    // After regexp escaping \\\\ = 1 slash, \\\\\\\\ = 2 slashes.

    // Also, the second args of replaceAll are neither strings nor regexps, and
    // are instead a special DSL used by Matcher. Therefore, we need to double
    // escape slashes (4 slashes) and quotes (3 slashes + ") in those strings.
    // Since we need to write \\ and \", we end up with 8 and 7 slashes,
    // respectively.
    return in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\\\"");
  }

  /**
   * Obtain the effective configuration of this ConfigurationBuilderImpl
   * instance. This consists of string-string pairs that could be written
   * directly to a Properties file, for example. Currently, this method does not
   * return information about default parameter values that were specified by
   * parameter annotations, or about the auto-discovered stuff in TypeHierarchy.
   * All of that should be automatically imported as these keys are parsed on
   * the other end.
   * 
   * @return A string containing enough information to rebuild this
   *         configuration object (assuming the same classes / jars are
   *         available when the string is parsed by Tang).
   */
  public static String toConfigurationString(final Configuration c) {
    ConfigurationImpl conf = (ConfigurationImpl) c;
    StringBuilder s = new StringBuilder();

    for (ClassNode<?> opt : conf.getBoundImplementations()) {
      s.append(opt.getFullName()).append('=')
              .append(conf.getBoundImplementation(opt).getFullName()).append('\n');
    }
    for (ClassNode<?> opt : conf.getBoundConstructors()) {
      s.append(opt.getFullName()).append('=')
              .append(conf.getBoundConstructor(opt).getFullName()).append('\n');
    }
    for (NamedParameterNode<?> opt : conf.getNamedParameters()) {
      s.append(opt.getFullName()).append('=')
              .append(escape(conf.getNamedParameter(opt))).append('\n');
    }
    for (ClassNode<?> cn : conf.getLegacyConstructors()) {
      s.append(cn.getFullName()).append('=').append(ConfigurationBuilderImpl.INIT).append('(');
      join(s, "-", conf.getLegacyConstructor(cn).getArgs()).append(")\n");
    }
    for (Entry<NamedParameterNode<Set<?>>,Object> e : conf.getBoundSets()) {
      final String val;
      if(e.getValue() instanceof String) {
        val = (String)e.getValue();
      } else if(e.getValue() instanceof Node) {
        val = ((Node)e.getValue()).getFullName();
      } else {
        throw new IllegalStateException();
      }
      s.append(e.getKey().getFullName()).append('=').append(val).append("\n");
    }
    return s.toString();
  }

  private static StringBuilder join(final StringBuilder sb, final String sep, final ConstructorArg[] types) {
    if (types.length > 0) {
      sb.append(types[0].getType());
      for (int i = 1; i < types.length; i++) {
        sb.append(sep).append(types[i].getType());
      }
    }
    return sb;
  }
}
