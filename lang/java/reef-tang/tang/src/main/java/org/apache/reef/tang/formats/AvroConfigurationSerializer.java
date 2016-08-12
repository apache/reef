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
package org.apache.reef.tang.formats;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.formats.avro.AvroConfiguration;
import org.apache.reef.tang.formats.avro.ConfigurationEntry;
import org.apache.reef.tang.implementation.ConfigurationBuilderImpl;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.types.Node;
import org.apache.reef.tang.util.ReflectionUtilities;

import javax.inject.Inject;

import java.io.*;
import java.util.*;

/**
 * (De-)Serializing Configuration to and from AvroConfiguration.
 * <p>
 * This class is stateless and is therefore safe to reuse.
 */
public final class AvroConfigurationSerializer implements ConfigurationSerializer {

  /**
   * The Charset used for the JSON encoding.
   * <p>
   * Copied from <code>org.apache.avro.io.JsonDecoder.CHARSET</code>
   */
  private static final String JSON_CHARSET = "ISO-8859-1";
  public static final String JAVA = "Java";
  public static final String CS = "Cs";

  @Inject
  public AvroConfigurationSerializer() {
  }

  private static void fromAvro(final AvroConfiguration avroConfiguration,
                               final ConfigurationBuilder configurationBuilder) throws BindException {
    // TODO[JIRA REEF-402]: This method should implement list deserialization. Implement it when C# side is ready.
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
        // TODO[JIRA REEF-402]: Implement list deserialization
        // rawValue is String.
        final String value = rawValue.toString();
        if (key.equals(ConfigurationBuilderImpl.IMPORT)) {
          configurationBuilder.getClassHierarchy().getNode(value);
          final String[] tok = value.split(ReflectionUtilities.REGEXP);
          final String lastTok = tok[tok.length - 1];
          try {
            configurationBuilder.getClassHierarchy().getNode(lastTok);
            throw new IllegalArgumentException("Conflict on short name: " + lastTok);
          } catch (final BindException e) {
            final String oldValue = importedNames.put(lastTok, value);
            if (oldValue != null) {
              throw new IllegalArgumentException("Name conflict: "
                  + lastTok + " maps to " + oldValue + " and " + value, e);
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

  private static AvroConfiguration avroFromBytes(final byte[] theBytes) throws IOException {
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(theBytes, null);
    final SpecificDatumReader<AvroConfiguration> reader = new SpecificDatumReader<>(AvroConfiguration.class);
    return reader.read(null, decoder);
  }

  private static AvroConfiguration avroFromString(final String theString) throws IOException {
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(AvroConfiguration.getClassSchema(), theString);
    final SpecificDatumReader<AvroConfiguration> reader = new SpecificDatumReader<>(AvroConfiguration.class);
    return reader.read(null, decoder);
  }

  public AvroConfiguration toAvro(final Configuration configuration) {
    // TODO[JIRA REEF-402]: This method should implement list serialization. Implement it when C# side is ready.

    final List<ConfigurationEntry> configurationEntries = new ArrayList<>();

    for (final ClassNode<?> opt : configuration.getBoundImplementations()) {
      configurationEntries.add(ConfigurationEntry.newBuilder()
          .setKey(opt.getFullName())
          .setValue(configuration.getBoundImplementation(opt).getFullName())
          .build());
    }

    for (final ClassNode<?> opt : configuration.getBoundConstructors()) {
      configurationEntries.add(ConfigurationEntry.newBuilder()
          .setKey(opt.getFullName())
          .setValue(configuration.getBoundConstructor(opt).getFullName())
          .build());
    }
    for (final NamedParameterNode<?> opt : configuration.getNamedParameters()) {
      configurationEntries.add(ConfigurationEntry.newBuilder()
          .setKey(opt.getFullName())
          .setValue(configuration.getNamedParameter(opt))
          .build());
    }
    for (final ClassNode<?> cn : configuration.getLegacyConstructors()) {
      final String legacyConstructors = StringUtils.join(configuration.getLegacyConstructor(cn).getArgs(), "-");
      configurationEntries.add(ConfigurationEntry.newBuilder()
          .setKey(cn.getFullName())
          .setValue("" + ConfigurationBuilderImpl.INIT + "(" + legacyConstructors + ")")
          .build());
    }
    for (final NamedParameterNode<Set<?>> key : configuration.getBoundSets()) {
      for (final Object value : configuration.getBoundSet(key)) {
        final String val;
        if (value instanceof String) {
          val = (String) value;
        } else if (value instanceof Node) {
          val = ((Node) value).getFullName();
        } else {
          throw new IllegalStateException("The value bound to a given NamedParameterNode "
                  + key + " is neither the set of class hierarchy nodes nor strings.");
        }
        configurationEntries.add(ConfigurationEntry.newBuilder()
            .setKey(key.getFullName())
            .setValue(val)
            .build());
      }
    }
    // TODO[JIRA REEF-402]: Implement list serialization
    if (configuration.getBoundLists() != null && !configuration.getBoundLists().isEmpty()) {
      throw new NotImplementedException("List serialization/deserialization is not supported");
    }

    return AvroConfiguration.newBuilder().setLanguage(JAVA).setBindings(configurationEntries).build();
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
    try (final Writer w = new OutputStreamWriter(new FileOutputStream(file), JSON_CHARSET)) {
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

  /**
   * Produce a JSON string that represents given configuration.
   * @param configuration Tang configuration to convert into a JSON string.
   * @return A JSON string that corresponds to the given Tang configuration.
   */
  @Override
  public String toString(final Configuration configuration) {
    return toString(configuration, false);
  }

  /**
   * Produce a JSON string that represents given configuration.
   * @param configuration Tang configuration to convert into a JSON string.
   * @param prettyPrint If true, use new lines and spaces to pretty print the JSON string.
   * If false (by default), output JSON as a single line.
   * @return A JSON string that corresponds to the given Tang configuration.
   */
  public String toString(final Configuration configuration, final boolean prettyPrint) {
    final DatumWriter<AvroConfiguration> configurationWriter = new SpecificDatumWriter<>(AvroConfiguration.class);
    final String result;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(AvroConfiguration.SCHEMA$, out, prettyPrint);
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
   * Converts a given AvroConfiguration to Configuration.
   *
   * @param avroConfiguration a Avro configuration
   * @return a Configuration version of the given AvroConfiguration
   */
  public Configuration fromAvro(final AvroConfiguration avroConfiguration) throws BindException {
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    fromAvro(avroConfiguration, configurationBuilder);
    return configurationBuilder.build();
  }

  /**
   * Converts a given AvroConfiguration to Configuration.
   *
   * @param avroConfiguration a Avro configuration
   * @param classHierarchy    the class hierarchy used for validation.
   * @return a Configuration version of the given AvroConfiguration
   */
  public Configuration fromAvro(final AvroConfiguration avroConfiguration, final ClassHierarchy classHierarchy)
      throws BindException {
    final ConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder(classHierarchy);
    fromAvro(avroConfiguration, configurationBuilder);
    return configurationBuilder.build();
  }

  @Override
  public Configuration fromFile(final File file) throws IOException, BindException {
    return fromAvro(avroFromFile(file));
  }

  @Override
  public Configuration fromFile(final File file, final ClassHierarchy classHierarchy)
      throws IOException, BindException {
    return fromAvro(avroFromFile(file), classHierarchy);
  }

  @Override
  public Configuration fromTextFile(final File file) throws IOException, BindException {
    final StringBuilder result = readFromTextFile(file);
    return this.fromString(result.toString());
  }

  @Override
  public Configuration fromTextFile(final File file, final ClassHierarchy classHierarchy)
      throws IOException, BindException {
    final StringBuilder result = readFromTextFile(file);
    return this.fromString(result.toString(), classHierarchy);
  }

  private StringBuilder readFromTextFile(final File file) throws IOException {
    final StringBuilder result = new StringBuilder();
    try (final BufferedReader reader =
             new BufferedReader(new InputStreamReader(new FileInputStream(file), JSON_CHARSET))) {
      String line = reader.readLine();
      while (line != null) {
        result.append(line);
        line = reader.readLine();
      }
    }
    return result;
  }

  @Override
  public Configuration fromByteArray(final byte[] theBytes) throws IOException, BindException {
    return fromAvro(avroFromBytes(theBytes));
  }

  @Override
  public Configuration fromByteArray(final byte[] theBytes, final ClassHierarchy classHierarchy)
      throws IOException, BindException {
    return fromAvro(avroFromBytes(theBytes), classHierarchy);
  }

  @Override
  public Configuration fromString(final String theString) throws IOException, BindException {
    return fromAvro(avroFromString(theString));
  }

  @Override
  public Configuration fromString(final String theString, final ClassHierarchy classHierarchy)
      throws IOException, BindException {
    return fromAvro(avroFromString(theString), classHierarchy);
  }

  /**
   * Converts a given serialized string to ConfigurationBuilder from which Configuration can be produced.
   *
   * @param theString the String containing configuration
   * @param configBuilder a configuration builder
   * @throws IOException if the string is not Avro format
   * @throws BindException if the content of configuration string is invalid to bind
   */
  public void configurationBuilderFromString(final String theString, final ConfigurationBuilder configBuilder)
      throws IOException, BindException {
    fromAvro(avroFromString(theString), configBuilder);
  }
}
