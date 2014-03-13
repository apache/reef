package com.microsoft.tang.formats;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.formats.avro.AvroConfiguration;
import com.microsoft.tang.formats.avro.ConfigurationEntry;
import com.microsoft.tang.implementation.ConfigurationBuilderImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.util.ReflectionUtilities;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * (De-)Serializing Configuration to and from AvroConfiguration
 */
public class AvroConfigurationSerializer {

  /**
   * Converts a given Configuration to AvroConfiguration
   *
   * @param configuration
   * @return an AvroConfiguration version of the given Configuration
   */
  public AvroConfiguration toAvro(final Configuration configuration) {
    // Note: This code is an adapted version of ConfiurationFile.toConfigurationStringList();


    final List<ConfigurationEntry> configurationEntries = new ArrayList<>();

    for (final ClassNode<?> opt : configuration.getBoundImplementations()) {
      configurationEntries.add(new ConfigurationEntry().newBuilder()
          .setKey(opt.getFullName())
          .setValue(configuration.getBoundImplementation(opt).getFullName())
          .build());
    }

    for (final ClassNode<?> opt : configuration.getBoundConstructors()) {
      configurationEntries.add(new ConfigurationEntry().newBuilder()
          .setKey(opt.getFullName())
          .setValue(configuration.getBoundConstructor(opt).getFullName())
          .build());
    }
    for (final NamedParameterNode<?> opt : configuration.getNamedParameters()) {
      configurationEntries.add(new ConfigurationEntry().newBuilder()
          .setKey(opt.getFullName())
          .setValue(configuration.getNamedParameter(opt))
          .build());
    }
    for (final ClassNode<?> cn : configuration.getLegacyConstructors()) {
      final String legacyConstructors = StringUtils.join(configuration.getLegacyConstructor(cn).getArgs(), "-");
      configurationEntries.add(new ConfigurationEntry().newBuilder()
          .setKey(cn.getFullName())
          .setValue("" + ConfigurationBuilderImpl.INIT + "(" + legacyConstructors + ")")
          .build());
    }
    for (final Map.Entry<NamedParameterNode<Set<?>>, Object> e : configuration.getBoundSets()) {
      final String val;
      if (e.getValue() instanceof String) {
        val = (String) e.getValue();
      } else if (e.getValue() instanceof Node) {
        val = ((Node) e.getValue()).getFullName();
      } else {
        throw new IllegalStateException();
      }
      configurationEntries.add(new ConfigurationEntry().newBuilder()
          .setKey(e.getKey().getFullName())
          .setValue(val)
          .build());
    }

    return AvroConfiguration.newBuilder().setBindings(configurationEntries).build();
  }

  /**
   * Converts a given AvroConfiguration to Configuration
   *
   * @param avroConfiguration
   * @return a Configuration version of the given AvroConfiguration
   */
  public Configuration fromAvro(final AvroConfiguration avroConfiguration) throws BindException {
    // Note: This code is an adapted version of ConfiurationFile.processConfigFile();
    final ConfigurationBuilderImpl ci = (ConfigurationBuilderImpl) Tang.Factory.getTang()
        .newConfigurationBuilder();
    final Map<String, String> importedNames = new HashMap<>();

    for (final ConfigurationEntry entry : avroConfiguration.getBindings()) {

      final String longName = importedNames.get(entry.getKey().toString());
      final String key;
      if (null == longName) {
        key = entry.getKey().toString();
      } else {
        key = longName;
      }

      final String value = entry.getValue().toString();

      try {
        if (key.equals(ConfigurationBuilderImpl.IMPORT)) {
          ci.getClassHierarchy().getNode(value);
          final String[] tok = value.split(ReflectionUtilities.regexp);
          final String lastTok = tok[tok.length - 1];
          try {
            ci.getClassHierarchy().getNode(lastTok);
            throw new IllegalArgumentException("Conflict on short name: " + lastTok);
          } catch (BindException e) {
            final String oldValue = importedNames.put(lastTok, value);
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
      } catch (BindException | ClassHierarchyException e) {
        throw new BindException("Failed to process configuration tuple: [" + key + "=" + value + "]", e);
      }
    }
    return ci.build();


  }


}
