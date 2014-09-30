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
package com.microsoft.tang.formats;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
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
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * (De-)Serializing Configuration to and from AvroConfiguration.
 * <p/>
 * This class is stateless and is therefore safe to reuse.
 */
public final class AvroConfigurationSerializer implements ConfigurationSerializer {

  /**
   * The Charset used for the JSON encoding.
   * <p/>
   * Copied from <code>org.apache.avro.io.JsonDecoder.CHARSET</code>
   */
  private static final String JSON_CHARSET = "ISO-8859-1";

  @Inject
  public AvroConfigurationSerializer() {
  }

  public AvroConfiguration toAvro(final Configuration configuration) {
    // Note: This code is an adapted version of ConfiurationFile.toConfigurationStringList();
    // TODO: This method should implement list serialization. Implement it when C# side is ready.

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
    // TODO: Implement list serialization

    return AvroConfiguration.newBuilder().setBindings(configurationEntries).build();
  }

  @Override
  public void toFile(final Configuration conf, final File file) throws IOException {
    final AvroConfiguration avroConfiguration = toAvro(conf);
    final DatumWriter<AvroConfiguration> configurationWriter = new SpecificDatumWriter<>(AvroConfiguration.class);
    try (DataFileWriter<AvroConfiguration> dataFileWriter = new DataFileWriter<>(configurationWriter)) {
      dataFileWriter.create(avroConfiguration.getSchema(), file);
      dataFileWriter.append(avroConfiguration);
    }
  }

  @Override
  public void toTextFile(final Configuration conf, final File file) throws IOException {
    try (final Writer w = new FileWriter(file)) {
      w.write(this.toString(conf));
    }
  }

  @Override
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

  @Override
  public String toString(final Configuration configuration) {
    final DatumWriter<AvroConfiguration> configurationWriter = new SpecificDatumWriter<>(AvroConfiguration.class);
    final String result;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(AvroConfiguration.SCHEMA$, out);
      configurationWriter.write(toAvro(configuration), encoder);
      encoder.flush();
      out.flush();
      result = out.toString(JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Converts a given AvroConfiguration to Configuration
   *
   * @param avroConfiguration
   * @return a Configuration version of the given AvroConfiguration
   */
  public Configuration fromAvro(final AvroConfiguration avroConfiguration) throws BindException {
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    fromAvro(avroConfiguration, configurationBuilder);
    return configurationBuilder.build();
  }

  /**
   * Converts a given AvroConfiguration to Configuration
   *
   * @param avroConfiguration
   * @param classHierarchy    the class hierarchy used for validation.
   * @return a Configuration version of the given AvroConfiguration
   */
  public Configuration fromAvro(final AvroConfiguration avroConfiguration, final ClassHierarchy classHierarchy)
      throws BindException {
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder(classHierarchy);
    fromAvro(avroConfiguration, configurationBuilder);
    return configurationBuilder.build();
  }

  private static void fromAvro(final AvroConfiguration avroConfiguration, final ConfigurationBuilder configurationBuilder) throws BindException {
    // Note: This code is an adapted version of ConfigurationFile.processConfigFile();
    // TODO: This method should implement list deserialization. Implement it when C# side is ready.
    final Map<String, String> importedNames = new HashMap<>();

    for (final ConfigurationEntry entry : avroConfiguration.getBindings()) {

      final String longName = importedNames.get(entry.getKey().toString());
      final String key;
      if (null == longName) {
        key = entry.getKey().toString();
      } else {
        key = longName;
      }

      // entry.getValue()'s type can be either string or array of string
      final Object rawValue = entry.getValue();

      try {
        // TODO: Implement list deserialization
        // rawValue is String.
        String value = rawValue.toString();
        if (key.equals(ConfigurationBuilderImpl.IMPORT)) {
          configurationBuilder.getClassHierarchy().getNode(value);
          final String[] tok = value.split(ReflectionUtilities.regexp);
          final String lastTok = tok[tok.length - 1];
          try {
            configurationBuilder.getClassHierarchy().getNode(lastTok);
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
          configurationBuilder.registerLegacyConstructor(key, classes);
        } else {
          configurationBuilder.bind(key, value);
        }
      } catch (final BindException | ClassHierarchyException e) {
        throw new BindException("Failed to process configuration tuple: [" + key + "=" + rawValue + "]", e);
      }
    }
  }

  private static AvroConfiguration avroFromFile(final File file) throws IOException {
    final AvroConfiguration avroConfiguration;
    try (final DataFileReader<AvroConfiguration> dataFileReader =
             new DataFileReader<>(file, new SpecificDatumReader<>(AvroConfiguration.class))) {
      avroConfiguration = dataFileReader.next();
    }
    return avroConfiguration;
  }

  @Override
  public Configuration fromFile(final File file) throws IOException, BindException {
    return fromAvro(avroFromFile(file));
  }

  @Override
  public Configuration fromFile(final File file, final ClassHierarchy classHierarchy) throws IOException, BindException {
    return fromAvro(avroFromFile(file), classHierarchy);
  }

  @Override
  public Configuration fromTextFile(final File file) throws IOException, BindException{
    final StringBuilder result = readFromTextFile(file);
    return this.fromString(result.toString());
  }

  @Override
  public Configuration fromTextFile(final File file, final ClassHierarchy classHierarchy) throws IOException, BindException{
    final StringBuilder result = readFromTextFile(file);
    return this.fromString(result.toString(), classHierarchy);
  }

  private StringBuilder readFromTextFile(final File file) throws IOException {
    final StringBuilder result = new StringBuilder();
    try (final BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line = reader.readLine();
      while (line != null) {
        result.append(line);
        line = reader.readLine();
      }
    }
    return result;
  }

  private static AvroConfiguration avroFromBytes(final byte[] theBytes) throws IOException {
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(theBytes, null);
    final SpecificDatumReader<AvroConfiguration> reader = new SpecificDatumReader<>(AvroConfiguration.class);
    return reader.read(null, decoder);
  }

  @Override
  public Configuration fromByteArray(final byte[] theBytes) throws IOException, BindException {
    return fromAvro(avroFromBytes(theBytes));
  }

  @Override
  public Configuration fromByteArray(final byte[] theBytes, final ClassHierarchy classHierarchy) throws IOException, BindException {
    return fromAvro(avroFromBytes(theBytes), classHierarchy);
  }

  private static AvroConfiguration avroFromString(final String theString) throws IOException {
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(AvroConfiguration.getClassSchema(), theString);
    final SpecificDatumReader<AvroConfiguration> reader = new SpecificDatumReader<>(AvroConfiguration.class);
    return reader.read(null, decoder);
  }

  @Override
  public Configuration fromString(final String theString) throws IOException, BindException {
    return fromAvro(avroFromString(theString));
  }

  @Override
  public Configuration fromString(final String theString, final ClassHierarchy classHierarchy) throws IOException, BindException {
    return fromAvro(avroFromString(theString), classHierarchy);
  }

}
