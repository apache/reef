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
package org.apache.reef.tang.implementation.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.ClassHierarchySerializer;
import org.apache.reef.tang.types.*;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Serialize and Deserialize ClassHierarchy to and from AvroClassHierarchy.
 * This class is stateless and is therefore safe to reuse.
 */
public final class AvroClassHierarchySerializer implements ClassHierarchySerializer {

  /**
   * The Charset used for the JSON encoding.
   */
  private static final String JSON_CHARSET = "ISO-8859-1";

  @Inject
  public AvroClassHierarchySerializer() {
  }

  /**
   * Serialize the ClassHierarchy into the AvroNode.
   * @param ch ClassHierarchy to serialize
   * @return a Avro node having class hierarchy
   */
  public AvroNode toAvro(final ClassHierarchy ch) {
    return newAvroNode(ch.getNamespace());
  }

  /**
   * Deserialize the ClassHierarchy from the AvroNode.
   * @param n AvroNode to deserialize
   * @return a class hierarchy
   */
  public ClassHierarchy fromAvro(final AvroNode n) {
    return new AvroClassHierarchy(n);
  }

  private AvroNode newAvroNode(final Node n) {
    final List<AvroNode> children = new ArrayList<>();
    for (final Node child : n.getChildren()) {
      children.add(newAvroNode(child));
    }
    if (n instanceof ClassNode) {
      final ClassNode<?> cn = (ClassNode<?>) n;
      final ConstructorDef<?>[] injectable = cn.getInjectableConstructors();
      final ConstructorDef<?>[] all = cn.getAllConstructors();
      final List<ConstructorDef<?>> others = new ArrayList<>(Arrays.asList(all));
      others.removeAll(Arrays.asList(injectable));

      final List<AvroConstructorDef> injectableConstructors = new ArrayList<>();
      for (final ConstructorDef<?> inj : injectable) {
        injectableConstructors.add(newConstructorDef(inj));
      }
      final List<AvroConstructorDef> otherConstructors = new ArrayList<>();
      for (final ConstructorDef<?> other : others) {
        otherConstructors.add(newConstructorDef(other));
      }
      final List<CharSequence> implFullNames = new ArrayList<>();
      for (final ClassNode<?> impl : cn.getKnownImplementations()) {
        implFullNames.add(impl.getFullName());
      }
      return newClassNode(n.getName(), n.getFullName(), cn.isInjectionCandidate(), cn.isExternalConstructor(),
              cn.isUnit(), injectableConstructors, otherConstructors, implFullNames, cn.getDefaultImplementation(),
              children);
    } else if (n instanceof NamedParameterNode) {
      final NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      final List<CharSequence> defaultInstances = new ArrayList<>();
      for (final CharSequence defaultInstance : np.getDefaultInstanceAsStrings()) {
        defaultInstances.add(defaultInstance);
      }
      return newNamedParameterNode(np.getName(), np.getFullName(), np.getSimpleArgName(), np.getFullArgName(),
              np.isSet(), np.isList(), np.getDocumentation(), np.getShortName(), defaultInstances, children);
    } else if (n instanceof PackageNode) {
      return newPackageNode(n.getName(), n.getFullName(), children);
    } else {
      throw new IllegalStateException("Encountered unknown type of Node: " + n);
    }
  }

  private AvroNode newClassNode(final String name,
                                       final String fullName,
                                       final boolean isInjectionCandidate,
                                       final boolean isExternalConstructor,
                                       final boolean isUnit,
                                       final List<AvroConstructorDef> injectableConstructors,
                                       final List<AvroConstructorDef> otherConstructors,
                                       final List<CharSequence> implFullNames,
                                       final String defaultImplementation,
                                       final List<AvroNode> children) {
    return AvroNode.newBuilder()
            .setName(name)
            .setFullName(fullName)
            .setClassNode(AvroClassNode.newBuilder()
                    .setIsInjectionCandidate(isInjectionCandidate)
                    .setIsExternalConstructor(isExternalConstructor)
                    .setIsUnit(isUnit)
                    .setInjectableConstructors(injectableConstructors)
                    .setOtherConstructors(otherConstructors)
                    .setImplFullNames(implFullNames)
                    .setDefaultImplementation(defaultImplementation)
                    .build())
            .setChildren(children).build();
  }

  private AvroNode newNamedParameterNode(final String name,
                                                final String fullName,
                                                final String simpleArgClassName,
                                                final String fullArgClassName,
                                                final boolean isSet,
                                                final boolean isList,
                                                final String documentation,
                                                final String shortName,
                                                final List<CharSequence> instanceDefault,
                                                final List<AvroNode> children) {

    return AvroNode.newBuilder()
            .setName(name)
            .setFullName(fullName)
            .setNamedParameterNode(AvroNamedParameterNode.newBuilder()
                    .setSimpleArgClassName(simpleArgClassName)
                    .setFullArgClassName(fullArgClassName)
                    .setIsSet(isSet)
                    .setIsList(isList)
                    .setDocumentation(documentation)
                    .setShortName(shortName)
                    .setInstanceDefault(instanceDefault)
                    .build())
            .setChildren(children).build();
  }

  private AvroNode newPackageNode(final String name,
                                         final String fullName,
                                         final List<AvroNode> children) {
    return AvroNode.newBuilder()
            .setPackageNode(AvroPackageNode.newBuilder().build())
            .setName(name).setFullName(fullName).setChildren(children).build();
  }

  private AvroConstructorArg newConstructorArg(final String fullArgClassName,
                                                      final String namedParameterName,
                                                      final boolean isFuture) {
    return AvroConstructorArg.newBuilder()
            .setFullArgClassName(fullArgClassName)
            .setIsInjectionFuture(isFuture)
            .setNamedParameterName(namedParameterName).build();
  }

  private AvroConstructorDef newConstructorDef(final ConstructorDef<?> def) {
    final List<AvroConstructorArg> args = new ArrayList<>();
    for (final ConstructorArg arg : def.getArgs()) {
      args.add(newConstructorArg(arg.getType(), arg.getNamedParameterName(), arg.isInjectionFuture()));
    }
    return AvroConstructorDef.newBuilder()
            .setFullClassName(def.getClassName())
            .setConstructorArgs(args).build();
  }

  @Override
  public void toFile(final ClassHierarchy classHierarchy, final File file) throws IOException {
    final AvroNode avroNode = toAvro(classHierarchy);
    final DatumWriter<AvroNode> avroNodeWriter = new SpecificDatumWriter<>(AvroNode.class);
    try (DataFileWriter<AvroNode> dataFileWriter = new DataFileWriter<>(avroNodeWriter)) {
      dataFileWriter.create(avroNode.getSchema(), file);
      dataFileWriter.append(avroNode);
    }
  }

  @Override
  public byte[] toByteArray(final ClassHierarchy classHierarchy) throws IOException {
    final DatumWriter<AvroNode> requestWriter = new SpecificDatumWriter<>(AvroNode.class);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      requestWriter.write(toAvro(classHierarchy), encoder);
      encoder.flush();
      return out.toByteArray();
    }
  }

  @Override
  public String toString(final ClassHierarchy classHierarchy) throws IOException {
    final DatumWriter<AvroNode> classHierarchyWriter = new SpecificDatumWriter<>(AvroNode.class);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(AvroNode.SCHEMA$, out);
      classHierarchyWriter.write(toAvro(classHierarchy), encoder);
      encoder.flush();
      out.flush();
      return out.toString(JSON_CHARSET);
    }
  }

  @Override
  public void toTextFile(final ClassHierarchy classHierarchy, final File file) throws IOException {
    try (Writer w = new FileWriter(file)) {
      w.write(this.toString(classHierarchy));
    }
  }

  @Override
  public ClassHierarchy fromFile(final File file) throws IOException {
    final AvroNode avroNode;
    try (DataFileReader<AvroNode> dataFileReader =
                 new DataFileReader<>(file, new SpecificDatumReader<>(AvroNode.class))) {
      avroNode = dataFileReader.next();
    }
    return fromAvro(avroNode);
  }

  @Override
  public ClassHierarchy fromByteArray(final byte[] theBytes) throws IOException {
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(theBytes, null);
    final SpecificDatumReader<AvroNode> reader = new SpecificDatumReader<>(AvroNode.class);
    return fromAvro(reader.read(null, decoder));
  }

  @Override
  public ClassHierarchy fromString(final String theString) throws IOException {
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(AvroNode.getClassSchema(), theString);
    final SpecificDatumReader<AvroNode> reader = new SpecificDatumReader<>(AvroNode.class);
    return fromAvro(reader.read(null, decoder));
  }

  @Override
  public ClassHierarchy fromTextFile(final File file) throws IOException {
    final StringBuilder stringBuilder = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line = reader.readLine();
      while (line != null) {
        stringBuilder.append(line);
        line = reader.readLine();
      }
    }
    return fromString(stringBuilder.toString());
  }
}
