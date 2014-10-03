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
package com.microsoft.tang.implementation.protobuf;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.types.ClassNodeImpl;
import com.microsoft.tang.implementation.types.ConstructorArgImpl;
import com.microsoft.tang.implementation.types.ConstructorDefImpl;
import com.microsoft.tang.implementation.types.NamedParameterNodeImpl;
import com.microsoft.tang.implementation.types.PackageNodeImpl;
import com.microsoft.tang.proto.ClassHierarchyProto;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;

public class ProtocolBufferClassHierarchy implements ClassHierarchy {

  private final PackageNode namespace;
  private static final String regex = "[\\.\\$\\+]";
  private HashMap<String, Node> lookupTable = new HashMap<>();

  // ############## Serialize implementation ############## 

  // protoc doesn't believe in auto-generating constructors, so here are
  // hand-generated ones. *sigh*

  private static ClassHierarchyProto.Node newClassNode(String name,
      String fullName, boolean isInjectionCandidate,
      boolean isExternalConstructor, boolean isUnit,
      List<ClassHierarchyProto.ConstructorDef> injectableConstructors,
      List<ClassHierarchyProto.ConstructorDef> otherConstructors,
      List<String> implFullNames, Iterable<ClassHierarchyProto.Node> children) {
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

  private static ClassHierarchyProto.Node newNamedParameterNode(String name,
      String fullName, String simpleArgClassName, String fullArgClassName,
      boolean isSet,
      boolean isList,
      String documentation, // can be null
      String shortName, // can be null
      String[] instanceDefault, // can be null
      Iterable<ClassHierarchyProto.Node> children) {
    ClassHierarchyProto.NamedParameterNode.Builder namedParameterNodeBuilder
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

  private static ClassHierarchyProto.Node newPackageNode(String name,
      String fullName, Iterable<ClassHierarchyProto.Node> children) {
    return ClassHierarchyProto.Node.newBuilder()
        .setPackageNode(ClassHierarchyProto.PackageNode.newBuilder().build())
        .setName(name).setFullName(fullName).addAllChildren(children).build();
  }

  private static ClassHierarchyProto.ConstructorDef newConstructorDef(
      String fullClassName, List<ClassHierarchyProto.ConstructorArg> args) {
    return ClassHierarchyProto.ConstructorDef.newBuilder()
        .setFullClassName(fullClassName).addAllArgs(args).build();
  }

  private static ClassHierarchyProto.ConstructorArg newConstructorArg(
      String fullArgClassName, String namedParameterName, boolean isFuture) {
    ClassHierarchyProto.ConstructorArg.Builder builder = 
        ClassHierarchyProto.ConstructorArg.newBuilder()
        .setFullArgClassName(fullArgClassName)
        .setIsInjectionFuture(isFuture);
    if(namedParameterName != null) {
        builder.setNamedParameterName(namedParameterName).build();
    }
    return builder.build();
  }

  // these serialize...() methods copy a pieces of the class hierarchy into
  // protobufs 
  
  private static ClassHierarchyProto.ConstructorDef serializeConstructorDef(
      ConstructorDef<?> def) {
    List<ClassHierarchyProto.ConstructorArg> args = new ArrayList<>();
    for (ConstructorArg arg : def.getArgs()) {
      args.add(newConstructorArg(arg.getType(), arg.getNamedParameterName(), arg.isInjectionFuture()));
    }
    return newConstructorDef(def.getClassName(), args);
  }

  private static ClassHierarchyProto.Node serializeNode(Node n) {
    List<ClassHierarchyProto.Node> children = new ArrayList<>();
    for (Node child : n.getChildren()) {
      children.add(serializeNode(child));
    }
    if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      ConstructorDef<?>[] injectable = cn.getInjectableConstructors();
      ConstructorDef<?>[] all = cn.getAllConstructors();
      List<ConstructorDef<?>> others = new ArrayList<>(Arrays.asList(all));
      others.removeAll(Arrays.asList(injectable));

      List<ClassHierarchyProto.ConstructorDef> injectableConstructors = new ArrayList<>();
      for (ConstructorDef<?> inj : injectable) {
        injectableConstructors.add(serializeConstructorDef(inj));
      }
      List<ClassHierarchyProto.ConstructorDef> otherConstructors = new ArrayList<>();
      for (ConstructorDef<?> other : others) {
        otherConstructors.add(serializeConstructorDef(other));
      }
      List<String> implFullNames = new ArrayList<>();
      for (ClassNode<?> impl : cn.getKnownImplementations()) {
        implFullNames.add(impl.getFullName());
      }
      return newClassNode(cn.getName(), cn.getFullName(),
          cn.isInjectionCandidate(), cn.isExternalConstructor(), cn.isUnit(),
          injectableConstructors, otherConstructors, implFullNames, children);
    } else if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
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
   * @param classHierarchy
   * @return
   */
  public static ClassHierarchyProto.Node serialize(ClassHierarchy classHierarchy) {
    return serializeNode(classHierarchy.getNamespace());
  }

  /**
   * serialize a class hierarchy into a file
   * @param file
   * @param classHierarchy
   * @throws IOException
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
   * @param file
   * @return
   * @throws IOException
   */
  public static ClassHierarchy deserialize(final File file) throws IOException {
    try (final InputStream stream = new FileInputStream(file)) {
      final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(stream);
      return new ProtocolBufferClassHierarchy(root);
    }
  }

  /**
   * Deserialize a class hierarchy from a protocol buffer object.  The resulting
   * object is immutable, and does not make use of reflection to fill in any
   * missing values.  This allows it to represent non-native classes as well
   * as snapshots of Java class hierarchies.
   */
  public ProtocolBufferClassHierarchy(ClassHierarchyProto.Node root) {
    namespace = new PackageNodeImpl();
    if (!root.hasPackageNode()) {
      throw new IllegalArgumentException("Expected a package node.  Got: "
          + root);
    }
    // Register all the classes.
    for (ClassHierarchyProto.Node child : root.getChildrenList()) {
      parseSubHierarchy(namespace, child);
    }
    buildLookupTable(namespace);
    // Now, register the implementations
    for (ClassHierarchyProto.Node child : root.getChildrenList()) {
      wireUpInheritanceRelationships(child);
    }
  }

    private void buildLookupTable(Node n)
    {
        for (Node child : n.getChildren()) {
            lookupTable.put(child.getFullName(), child);
            buildLookupTable(child);
        }
    }

  private static void parseSubHierarchy(Node parent, ClassHierarchyProto.Node n) {
    final Node parsed;
    if (n.hasPackageNode()) {
      parsed = new PackageNodeImpl(parent, n.getName(), n.getFullName());
    } else if (n.hasNamedParameterNode()) {
      ClassHierarchyProto.NamedParameterNode np = n.getNamedParameterNode();
      parsed = new NamedParameterNodeImpl<Object>(parent, n.getName(),
          n.getFullName(), np.getFullArgClassName(), np.getSimpleArgClassName(),
          np.getIsSet(), np.getIsList(), np.getDocumentation(), np.getShortName(),
          np.getInstanceDefaultList().toArray(new String[0]));
    } else if (n.hasClassNode()) {
      ClassHierarchyProto.ClassNode cn = n.getClassNode();
      List<ConstructorDef<?>> injectableConstructors = new ArrayList<>();
      List<ConstructorDef<?>> allConstructors = new ArrayList<>();

      for (ClassHierarchyProto.ConstructorDef injectable : cn
          .getInjectableConstructorsList()) {
        ConstructorDef<?> def = parseConstructorDef(injectable, true);
        injectableConstructors.add(def);
        allConstructors.add(def);
      }
      for (ClassHierarchyProto.ConstructorDef other : cn
          .getOtherConstructorsList()) {
        ConstructorDef<?> def = parseConstructorDef(other, false);
        allConstructors.add(def);

      }
      @SuppressWarnings("unchecked")
      ConstructorDef<Object>[] dummy = new ConstructorDef[0];
      parsed = new ClassNodeImpl<>(parent, n.getName(), n.getFullName(),
          cn.getIsUnit(), cn.getIsInjectionCandidate(),
          cn.getIsExternalConstructor(), injectableConstructors.toArray(dummy),
          allConstructors.toArray(dummy), cn.getDefaultImplementation());
    } else {
      throw new IllegalStateException("Bad protocol buffer: got abstract node"
          + n);
    }
    for (ClassHierarchyProto.Node child : n.getChildrenList()) {
      parseSubHierarchy(parsed, child);
    }
  }

  private static ConstructorDef<?> parseConstructorDef(
      com.microsoft.tang.proto.ClassHierarchyProto.ConstructorDef def,
      boolean isInjectable) {
    List<ConstructorArg> args = new ArrayList<>();
    for (ClassHierarchyProto.ConstructorArg arg : def.getArgsList()) {
      args.add(new ConstructorArgImpl(arg.getFullArgClassName(), arg
          .getNamedParameterName(),arg.getIsInjectionFuture()));
    }
    return new ConstructorDefImpl<>(def.getFullClassName(),
        args.toArray(new ConstructorArg[0]), isInjectable);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void wireUpInheritanceRelationships(final ClassHierarchyProto.Node n) {
    if (n.hasClassNode()) {
      final ClassHierarchyProto.ClassNode cn = n.getClassNode();
      final ClassNode iface;
      try {
        iface = (ClassNode) getNode(n.getFullName());
      } catch (NameResolutionException e) {
        throw new IllegalStateException("When reading protocol buffer node "
            + n.getFullName() + " does not exist.  Full record is " + n, e);
      }
      for (String impl : cn.getImplFullNamesList()) {
        try {
          iface.putImpl((ClassNode) getNode(impl));
        } catch (NameResolutionException e) {
          throw new IllegalStateException("When reading protocol buffer node "
              + n + " refers to non-existent implementation:" + impl);
        } catch (ClassCastException e) {
          try {
            throw new IllegalStateException(
                "When reading protocol buffer node " + n
                    + " found implementation" + getNode(impl)
                    + " which is not a ClassNode!");
          } catch (NameResolutionException e2) {
            throw new IllegalStateException(
                "Got 'cant happen' exception when producing error message for "
                    + e);
          }
        }
      }
    }

    for (ClassHierarchyProto.Node child : n.getChildrenList()) {
      wireUpInheritanceRelationships(child);
    }
  }

  private static String getNthPrefix(String str, int n) {
    n++; // want this function to be zero indexed...
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == '.' || c == '$' || c == '+') {
        n--;
      }
      if (n == 0) {
        return str.substring(0, i);
      }
    }
    if(n == 1) {
      return str;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public Node getNode(String fullName) throws NameResolutionException {

     Node ret = lookupTable.get(fullName);
/*    String[] tok = fullName.split(regex);

    Node ret = namespace.get(fullName);
    for (int i = 0; i < tok.length; i++) {
      Node n = namespace.get(getNthPrefix(fullName, i));
      if (n != null) {
        for (i++; i < tok.length; i++) {
          n = n.get(tok[i]);
          if (n == null) {
            throw new NameResolutionException(fullName, getNthPrefix(fullName,
                i - 1));
          }
        }
        return n;
      }
    } */
    if(ret != null) {
      return ret;
    } else {
      throw new NameResolutionException(fullName, "");
    }
  }

  @Override
  public boolean isImplementation(ClassNode<?> inter, ClassNode<?> impl) {
    return impl.isImplementationOf(inter);
  }

  @Override
  public ClassHierarchy merge(ClassHierarchy ch) {
    if (this == ch) {
      return this;
    }
    throw new UnsupportedOperationException(
        "Cannot merge ExternalClassHierarchies yet!");
  }

  @Override
  public Node getNamespace() {
    return namespace;
  }

}
