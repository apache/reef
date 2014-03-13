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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * (De-)Serializing Configuration to and from AvroConfiguration.
 * <p/>
 * This class is stateless and is therefore safe to reuse.
 */
public final class AvroConfigurationSerializer {

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
   * Stores the given Configuration in the given File.
   *
   * @param conf the Configuration to store
   * @param file the file to store the Configuration in
   * @throws java.io.IOException if there is an IO error in the process.
   */
  public void toFile(final Configuration conf, final File file) throws IOException {
    final AvroConfiguration avroConfiguration = toAvro(conf);
    final DatumWriter<AvroConfiguration> configurationWriter = new SpecificDatumWriter<>(AvroConfiguration.class);
    try (DataFileWriter<AvroConfiguration> dataFileWriter = new DataFileWriter<>(configurationWriter)) {
      dataFileWriter.create(avroConfiguration.getSchema(), file);
      dataFileWriter.append(avroConfiguration);
    }
  }

  /**
   * Writes the Configuration to a byte[].
   *
   * @param conf
   * @return
   * @throws IOException
   */
  public byte[] toByteArray(final Configuration conf) throws IOException {
    final DatumWriter<AvroConfiguration> configurationWriter = new SpecificDatumWriter<>(AvroConfiguration.class);
    final byte[] theBytes;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      configurationWriter.write(toAvro(conf), encoder);
      encoder.flush();
      out.flush();
      theBytes = out.toByteArray();
    }
    return theBytes;

  }

  /**
   * Converts a given AvroConfiguration to Configuration
   *
   * @param avroConfiguration
   * @return a Configuration version of the given AvroConfiguration
   */
  public Configuration fromAvro(final AvroConfiguration avroConfiguration) throws BindException {
    // Note: This code is an adapted version of ConfigurationFile.processConfigFile();
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
          } catch (final BindException e) {
            final String oldValue = importedNames.put(lastTok, value);
            if (oldValue != null) {
              throw new IllegalArgumentException("Name conflict: "
                  + lastTok + " maps to " + oldValue + " and " + value);
            }
          }
        } else if (value.startsWith(ConfigurationBuilderImpl.INIT)) {
          final String[] classes = value.substring(ConfigurationBuilderImpl.INIT.length(), value.length())
              .replaceAll("^[\\s\\(]+", "")
              .replaceAll("[\\s\\)]+$", "")
              .split("[\\s\\-]+");
          ci.registerLegacyConstructor(key, classes);
        } else {
          ci.bind(key, value);
        }
      } catch (final BindException | ClassHierarchyException e) {
        throw new BindException("Failed to process configuration tuple: [" + key + "=" + value + "]", e);
      }
    }
    return ci.build();
  }

  /**
   * Loads a Configuration from a File created with toFile().
   *
   * @param file the File to read from.
   * @return the Configuration stored in the file.
   * @throws IOException   if the File can't be read or parsed
   * @throws BindException if the file contains an illegal Configuration
   */
  public Configuration fromFile(final File file) throws IOException, BindException {
    final AvroConfiguration avroConfiguration;
    try (final DataFileReader<AvroConfiguration> dataFileReader =
             new DataFileReader<>(file, new SpecificDatumReader<>(AvroConfiguration.class))) {
      avroConfiguration = dataFileReader.next();
    }
    return fromAvro(avroConfiguration);
  }

  /**
   * Loads a Configuration from a byte[] created with toByteArray().
   *
   * @param theBytes the bytes to deserialize.
   * @return the Configuration stored.
   * @throws IOException   if the byte[] can't be deserialized
   * @throws BindException if the byte[] contains an illegal Configuration.
   */

  public Configuration fromByteArray(final byte[] theBytes) throws IOException, BindException {
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(theBytes, null);
    final SpecificDatumReader<AvroConfiguration> reader = new SpecificDatumReader<>(AvroConfiguration.class);
    return fromAvro(reader.read(null, decoder));
  }


}
