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
package org.apache.reef.tang.implementation.protobuf;

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.implementation.types.*;
import org.apache.reef.tang.proto.ClassHierarchyProto;
import org.apache.reef.tang.types.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @deprecated in 0.12. Use AvroClassHierarchy instead
 */
@Deprecated
public class ProtocolBufferClassHierarchy implements ClassHierarchy {

  private final PackageNode namespace;
  private HashMap<String, Node> lookupTable = new HashMap<>();

  // ############## Serialize implementation ############## 

  // protoc doesn't believe in auto-generating constructors, so here are
  // hand-generated ones. *sigh*

  /**
   * Deserialize a class hierarchy from a protocol buffer object.  The resulting
   * object is immutable, and does not make use of reflection to fill in any
   * missing values.  This allows it to represent non-native classes as well
   * as snapshots of Java class hierarchies.
   * @deprecated in 0.12. Use AvroClassHierarchy instead
   */
  public ProtocolBufferClassHierarchy(final ClassHierarchyProto.Node root) {
    namespace = new PackageNodeImpl();
    if (!root.hasPackageNode()) {
      throw new IllegalArgumentException("Expected a package node.  Got: "
          + root);
    }
    // Register all the classes.
    for (final ClassHierarchyProto.Node child : root.getChildrenList()) {
      parseSubHierarchy(namespace, child);
    }
    buildLookupTable(namespace);
    // Now, register the implementations
    for (final ClassHierarchyProto.Node child : root.getChildrenList()) {
      wireUpInheritanceRelationships(child);
    }
  }

  private static ClassHierarchyProto.Node newClassNode(
      final String name, final String fullName, final boolean isInjectionCandidate,
      final boolean isExternalConstructor, final boolean isUnit,
      final List<ClassHierarchyProto.ConstructorDef> injectableConstructors,
      final List<ClassHierarchyProto.ConstructorDef> otherConstructors, final List<String> implFullNames,
      final Iterable<ClassHierarchyProto.Node> children) {
    return ClassHierarchyProto.Node
        .newBuilder()
        .setName(name)
        .setFullName(fullName)
        .setClassNode(
            ClassHierarchyProto.ClassNode.newBuilder()
                .setIsInjectionCandidate(isInjectionCandidate)
                .setIsExternalConstructor(isExternalConstructor)
                .setIsUnit(isUnit)
                .addAllInjectableConstructors(injectableConstructors)
                .addAllOtherConstructors(otherConstructors)
                .addAllImplFullNames(implFullNames).build())
        .addAllChildren(children).build();
  }

  private static ClassHierarchyProto.Node newNamedParameterNode(final String name,
                                                                final String fullName,
                                                                final String simpleArgClassName,
                                                                final String fullArgClassName,
                                                                final boolean isSet,
                                                                final boolean isList,
                                                                final String documentation, // can be null
                                                                final String shortName, // can be null
                                                                final String[] instanceDefault, // can be null
                                                                final Iterable<ClassHierarchyProto.Node> children) {
    final ClassHierarchyProto.NamedParameterNode.Builder namedParameterNodeBuilder
        = ClassHierarchyProto.NamedParameterNode.newBuilder()
        .setSimpleArgClassName(simpleArgClassName)
        .setFullArgClassName(fullArgClassName)
        .setIsSet(isSet)
        .setIsList(isList);
    if (documentation != null) {
      namedParameterNodeBuilder.setDocumentation(documentation);
    }
    if (shortName != null) {
      namedParameterNodeBuilder.setShortName(shortName);
    }
    if (instanceDefault != null) {
      namedParameterNodeBuilder.addAllInstanceDefault(Arrays.asList(instanceDefault));
    }

    return ClassHierarchyProto.Node.newBuilder().setName(name)
        .setFullName(fullName)
        .setNamedParameterNode(namedParameterNodeBuilder.build())
        .addAllChildren(children).build();
  }

  private static ClassHierarchyProto.Node newPackageNode(final String name,
                                                         final String fullName,
                                                         final Iterable<ClassHierarchyProto.Node> children) {
    return ClassHierarchyProto.Node.newBuilder()
        .setPackageNode(ClassHierarchyProto.PackageNode.newBuilder().build())
        .setName(name).setFullName(fullName).addAllChildren(children).build();
  }

  private static ClassHierarchyProto.ConstructorDef newConstructorDef(
      final String fullClassName, final List<ClassHierarchyProto.ConstructorArg> args) {
    return ClassHierarchyProto.ConstructorDef.newBuilder()
        .setFullClassName(fullClassName).addAllArgs(args).build();
  }

  // these serialize...() methods copy a pieces of the class hierarchy into
  // protobufs 

  private static ClassHierarchyProto.ConstructorArg newConstructorArg(
      final String fullArgClassName, final String namedParameterName, final boolean isFuture) {
    final ClassHierarchyProto.ConstructorArg.Builder builder =
        ClassHierarchyProto.ConstructorArg.newBuilder()
            .setFullArgClassName(fullArgClassName)
            .setIsInjectionFuture(isFuture);
    if (namedParameterName != null) {
      builder.setNamedParameterName(namedParameterName).build();
    }
    return builder.build();
  }

  private static ClassHierarchyProto.ConstructorDef serializeConstructorDef(
      final ConstructorDef<?> def) {
    final List<ClassHierarchyProto.ConstructorArg> args = new ArrayList<>();
    for (final ConstructorArg arg : def.getArgs()) {
      args.add(newConstructorArg(arg.getType(), arg.getNamedParameterName(), arg.isInjectionFuture()));
    }
    return newConstructorDef(def.getClassName(), args);
  }

  private static ClassHierarchyProto.Node serializeNode(final Node n) {
    final List<ClassHierarchyProto.Node> children = new ArrayList<>();
    for (final Node child : n.getChildren()) {
      children.add(serializeNode(child));
    }
    if (n instanceof ClassNode) {
      final ClassNode<?> cn = (ClassNode<?>) n;
      final ConstructorDef<?>[] injectable = cn.getInjectableConstructors();
      final ConstructorDef<?>[] all = cn.getAllConstructors();
      final List<ConstructorDef<?>> others = new ArrayList<>(Arrays.asList(all));
      others.removeAll(Arrays.asList(injectable));

      final List<ClassHierarchyProto.ConstructorDef> injectableConstructors = new ArrayList<>();
      for (final ConstructorDef<?> inj : injectable) {
        injectableConstructors.add(serializeConstructorDef(inj));
      }
      final List<ClassHierarchyProto.ConstructorDef> otherConstructors = new ArrayList<>();
      for (final ConstructorDef<?> other : others) {
        otherConstructors.add(serializeConstructorDef(other));
      }
      final List<String> implFullNames = new ArrayList<>();
      for (final ClassNode<?> impl : cn.getKnownImplementations()) {
        implFullNames.add(impl.getFullName());
      }
      return newClassNode(cn.getName(), cn.getFullName(),
          cn.isInjectionCandidate(), cn.isExternalConstructor(), cn.isUnit(),
          injectableConstructors, otherConstructors, implFullNames, children);
    } else if (n instanceof NamedParameterNode) {
      final NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      return newNamedParameterNode(np.getName(), np.getFullName(),
          np.getSimpleArgName(), np.getFullArgName(), np.isSet(), np.isList(), np.getDocumentation(),
          np.getShortName(), np.getDefaultInstanceAsStrings(), children);
    } else if (n instanceof PackageNode) {
      return newPackageNode(n.getName(), n.getFullName(), children);
    } else {
      throw new IllegalStateException("Encountered unknown type of Node: " + n);
    }
  }

  /**
   * Serialize a class hierarchy into a protocol buffer object.
   *
   * @param classHierarchy
   * @return
   * @deprecated in 0.12. Use AvroClassHierarchySerializer instead
   */
  public static ClassHierarchyProto.Node serialize(final ClassHierarchy classHierarchy) {
    return serializeNode(classHierarchy.getNamespace());
  }

  /**
   * serialize a class hierarchy into a file.
   *
   * @param file
   * @param classHierarchy
   * @throws IOException
   * @deprecated in 0.12. Use AvroClassHierarchySerializer instead
   */
  public static void serialize(final File file, final ClassHierarchy classHierarchy) throws IOException {
    final ClassHierarchyProto.Node node = serializeNode(classHierarchy.getNamespace());
    try (final FileOutputStream output = new FileOutputStream(file)) {
      try (final DataOutputStream dos = new DataOutputStream(output)) {
        node.writeTo(dos);
      }
    }
  }

  /**
   * Deserialize a class hierarchy from a file. The file can be generated from either Java or C#
   *
   * @param file
   * @return
   * @throws IOException
   * @deprecated in 0.12. Use AvroClassHierarchySerializer instead
   */
  public static ClassHierarchy deserialize(final File file) throws IOException {
    try (final InputStream stream = new FileInputStream(file)) {
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(stream);
      return new ProtocolBufferClassHierarchy(root);
    }
  }

  private static void parseSubHierarchy(final Node parent, final ClassHierarchyProto.Node n) {
    final Node parsed;
    if (n.hasPackageNode()) {
      parsed = new PackageNodeImpl(parent, n.getName(), n.getFullName());
    } else if (n.hasNamedParameterNode()) {
      final ClassHierarchyProto.NamedParameterNode np = n.getNamedParameterNode();
      parsed = new NamedParameterNodeImpl<Object>(parent, n.getName(),
          n.getFullName(), np.getFullArgClassName(), np.getSimpleArgClassName(),
          np.getIsSet(), np.getIsList(), np.getDocumentation(), np.getShortName(),
          np.getInstanceDefaultList().toArray(new String[0]));
    } else if (n.hasClassNode()) {
      final ClassHierarchyProto.ClassNode cn = n.getClassNode();
      final List<ConstructorDef<?>> injectableConstructors = new ArrayList<>();
      final List<ConstructorDef<?>> allConstructors = new ArrayList<>();

      for (final ClassHierarchyProto.ConstructorDef injectable : cn
          .getInjectableConstructorsList()) {
        final ConstructorDef<?> def = parseConstructorDef(injectable, true);
        injectableConstructors.add(def);
        allConstructors.add(def);
      }
      for (final ClassHierarchyProto.ConstructorDef other : cn
          .getOtherConstructorsList()) {
        final ConstructorDef<?> def = parseConstructorDef(other, false);
        allConstructors.add(def);

      }
      @SuppressWarnings("unchecked") final ConstructorDef<Object>[] dummy = new ConstructorDef[0];
      parsed = new ClassNodeImpl<>(parent, n.getName(), n.getFullName(),
          cn.getIsUnit(), cn.getIsInjectionCandidate(),
          cn.getIsExternalConstructor(), injectableConstructors.toArray(dummy),
          allConstructors.toArray(dummy), cn.getDefaultImplementation());
    } else {
      throw new IllegalStateException("Bad protocol buffer: got abstract node"
          + n);
    }
    for (final ClassHierarchyProto.Node child : n.getChildrenList()) {
      parseSubHierarchy(parsed, child);
    }
  }

  private static ConstructorDef<?> parseConstructorDef(
      final ClassHierarchyProto.ConstructorDef def,
      final boolean isInjectable) {
    final List<ConstructorArg> args = new ArrayList<>();
    for (final ClassHierarchyProto.ConstructorArg arg : def.getArgsList()) {
      args.add(new ConstructorArgImpl(arg.getFullArgClassName(), arg
          .getNamedParameterName(), arg.getIsInjectionFuture()));
    }
    return new ConstructorDefImpl<>(def.getFullClassName(),
        args.toArray(new ConstructorArg[0]), isInjectable);
  }

  private static String getNthPrefix(final String str, final int n) {
    int j = n;
    j++; // want this function to be zero indexed...
    for (int i = 0; i < str.length(); i++) {
      final char c = str.charAt(i);
      if (c == '.' || c == '$' || c == '+') {
        j--;
      }
      if (j == 0) {
        return str.substring(0, i);
      }
    }
    if (j == 1) {
      return str;
    } else {
      throw new ArrayIndexOutOfBoundsException("The value of j should be 1, but actually is " + j);
    }
  }

  private void buildLookupTable(final Node n) {
    for (final Node child : n.getChildren()) {
      lookupTable.put(child.getFullName(), child);
      buildLookupTable(child);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void wireUpInheritanceRelationships(final ClassHierarchyProto.Node n) {
    if (n.hasClassNode()) {
      final ClassHierarchyProto.ClassNode cn = n.getClassNode();
      final ClassNode iface;
      try {
        iface = (ClassNode) getNode(n.getFullName());
      } catch (final NameResolutionException e) {
        throw new IllegalStateException("When reading protocol buffer node "
            + n.getFullName() + " does not exist.  Full record is " + n, e);
      }
      for (final String impl : cn.getImplFullNamesList()) {
        try {
          iface.putImpl((ClassNode) getNode(impl));
        } catch (final NameResolutionException e) {
          throw new IllegalStateException("When reading protocol buffer node "
              + n + " refers to non-existent implementation:" + impl, e);
        } catch (final ClassCastException e) {
          try {
            throw new IllegalStateException(
                "When reading protocol buffer node " + n
                    + " found implementation" + getNode(impl)
                    + " which is not a ClassNode!");
          } catch (final NameResolutionException e2) {
            throw new IllegalStateException(
                "Got 'cant happen' exception when producing error message for "
                    + e, e2);
          }
        }
      }
    }

    for (final ClassHierarchyProto.Node child : n.getChildrenList()) {
      wireUpInheritanceRelationships(child);
    }
  }

  @Override
  public Node getNode(final String fullName) throws NameResolutionException {

    final Node ret = lookupTable.get(fullName);
    if (ret != null) {
      return ret;
    } else {
      throw new NameResolutionException(fullName, "");
    }
  }

  @Override
  public boolean isImplementation(final ClassNode<?> inter, final ClassNode<?> impl) {
    return impl.isImplementationOf(inter);
  }

  @Override
  public ClassHierarchy merge(final ClassHierarchy ch) {
    if (this == ch) {
      return this;
    }
    if (!(ch instanceof ProtocolBufferClassHierarchy)) {
      throw new UnsupportedOperationException(
          "Cannot merge with class hierarchies of type: " + ch.getClass().getName());
    }

    final ProtocolBufferClassHierarchy pch = (ProtocolBufferClassHierarchy) ch;
    for (final String key : pch.lookupTable.keySet()) {
      if (!this.lookupTable.containsKey(key)) {
        this.lookupTable.put(key, pch.lookupTable.get(key));
      }

      for (final Node n : ch.getNamespace().getChildren()) {
        if (!this.namespace.contains(n.getFullName())) {
          if (n instanceof NamedParameter) {
            final NamedParameterNode np = (NamedParameterNode) n;
            new NamedParameterNodeImpl<>(this.namespace, np.getName(),
                np.getFullName(), np.getFullArgName(), np.getSimpleArgName(),
                np.isSet(), np.isList(), np.getDocumentation(), np.getShortName(),
                np.getDefaultInstanceAsStrings());
          } else if (n instanceof ClassNode) {
            final ClassNode cn = (ClassNode) n;
            new ClassNodeImpl(namespace, cn.getName(), cn.getFullName(),
                cn.isUnit(), cn.isInjectionCandidate(),
                cn.isExternalConstructor(), cn.getInjectableConstructors(),
                cn.getAllConstructors(), cn.getDefaultImplementation());
          }
        }
      }
    }
    return this;
  }

  @Override
  public Node getNamespace() {
    return namespace;
  }
}
