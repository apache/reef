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

import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.implementation.types.*;
import org.apache.reef.tang.types.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Implementation of ClassHierarchy for Avro.
 */
final class AvroClassHierarchy implements ClassHierarchy {
  private final PackageNode namespace;
  private final HashMap<String, Node> lookupTable = new HashMap<>();

  AvroClassHierarchy(final AvroNode root) {
    namespace = new PackageNodeImpl();
    if (root.getPackageNode() == null) {
      throw new IllegalArgumentException("Expected a package node. Got: " + root);
    }

    // Register all the classes.
    for (final AvroNode child : root.getChildren()) {
      parseSubHierarchy(namespace, child);
    }
    buildLookupTable(namespace);

    // Register the implementations
    for (final AvroNode child : root.getChildren()) {
      wireUpInheritanceRelationships(child);
    }
  }

  /**
   * Build a table that maps the name to the corresponding Node recursively.
   */
  private void buildLookupTable(final Node n) {
    for (final Node child : n.getChildren()) {
      lookupTable.put(child.getFullName(), child);
      buildLookupTable(child);
    }
  }

  /**
   * Parse the constructor definition.
   */
  private ConstructorDef<?> parseConstructorDef(final AvroConstructorDef def, final boolean isInjectable) {
    final List<ConstructorArg> args = new ArrayList<>();
    for (final AvroConstructorArg arg : def.getConstructorArgs()) {
      args.add(new ConstructorArgImpl(getString(arg.getFullArgClassName()), getString(arg.getNamedParameterName()),
              arg.getIsInjectionFuture()));
    }
    return new ConstructorDefImpl<>(getString(def.getFullClassName()), args.toArray(new ConstructorArg[0]),
            isInjectable);

  }

  /**
   * Register the classes recursively.
   */
  private void parseSubHierarchy(final Node parent, final AvroNode n) {
    final Node parsed;
    if (n.getPackageNode() != null) {
      parsed = new PackageNodeImpl(parent, getString(n.getName()), getString(n.getFullName()));
    } else if (n.getNamedParameterNode() != null) {
      final AvroNamedParameterNode np = n.getNamedParameterNode();


      parsed = new NamedParameterNodeImpl<>(parent, getString(n.getName()), getString(n.getFullName()),
              getString(np.getFullArgClassName()), getString(np.getSimpleArgClassName()), np.getIsSet(), np.getIsList(),
              getString(np.getDocumentation()), getString(np.getShortName()), getStringArray(np.getInstanceDefault()));
    } else if (n.getClassNode() != null) {
      final AvroClassNode cn = n.getClassNode();
      final List<ConstructorDef<?>> injectableConstructors = new ArrayList<>();
      final List<ConstructorDef<?>> allConstructors = new ArrayList<>();

      for (final AvroConstructorDef injectable : cn.getInjectableConstructors()) {
        final ConstructorDef<?> def = parseConstructorDef(injectable, true);
        injectableConstructors.add(def);
        allConstructors.add(def);
      }
      for (final AvroConstructorDef other : cn.getOtherConstructors()) {
        final ConstructorDef<?> def = parseConstructorDef(other, false);
        allConstructors.add(def);
      }
      @SuppressWarnings("unchecked")
      final ConstructorDef<Object>[] dummy = new ConstructorDef[0];
      parsed = new ClassNodeImpl<>(parent, getString(n.getName()), getString(n.getFullName()), cn.getIsUnit(),
              cn.getIsInjectionCandidate(), cn.getIsExternalConstructor(), injectableConstructors.toArray(dummy),
              allConstructors.toArray(dummy), getString(cn.getDefaultImplementation()));
    } else {
      throw new IllegalStateException("Bad avro node: got abstract node" + n);
    }

    for (final AvroNode child : n.getChildren()) {
      parseSubHierarchy(parsed, child);
    }
  }

  /**
   * Register the implementation for the ClassNode recursively.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void wireUpInheritanceRelationships(final AvroNode n) {
    if (n.getClassNode() != null) {
      final AvroClassNode cn = n.getClassNode();
      final ClassNode iface;
      try {
        iface = (ClassNode) getNode(getString(n.getFullName()));
      } catch (final NameResolutionException e) {
        final String errorMessage = new StringBuilder()
                .append("When reading avro node ").append(n.getFullName())
                .append(" does not exist.  Full record is ").append(n).toString();
        throw new IllegalStateException(errorMessage, e);
      }
      for (final CharSequence impl : cn.getImplFullNames()) {
        try {
          iface.putImpl((ClassNode) getNode(getString(impl)));
        } catch (final NameResolutionException e) {
          final String errorMessage = new StringBuilder()
                  .append("When reading avro node ").append(n)
                  .append(" refers to non-existent implementation:").append(impl).toString();
          throw new IllegalStateException(errorMessage, e);
        } catch (final ClassCastException e) {
          try {
            final String errorMessage = new StringBuilder()
                    .append("When reading avro node ").append(n).append(" found implementation")
                    .append(getNode(getString(impl))).append(" which is not a ClassNode!").toString();
            throw new IllegalStateException(errorMessage, e);
          } catch (final NameResolutionException e2) {
            final String errorMessage = new StringBuilder()
                    .append("Got 'cant happen' exception when producing error message for ")
                    .append(e).toString();
            throw new IllegalStateException(errorMessage, e2);
          }
        }
      }
    }

    for (final AvroNode child : n.getChildren()) {
      wireUpInheritanceRelationships(child);
    }
  }

  /**
   * Get the value of the CharSequence. Return null if the CharSequence is null.
   */
  private String getString(final CharSequence charSeq) {
    if (charSeq == null) {
      return null;
    } else {
      return charSeq.toString();
    }
  }

  /**
   * Convert the CharSequence list into the String array.
   */
  private String[] getStringArray(final List<CharSequence> charSeqList) {
    final int length = charSeqList.size();
    final String[] stringArray = new String[length];
    for (int i = 0; i < length; i++) {
      stringArray[i] = getString(charSeqList.get(i));
    }
    return stringArray;
  }

  @Override
  public Node getNode(final String fullName) throws NameResolutionException {
    final Node matchedNode = lookupTable.get(fullName);
    if (matchedNode != null) {
      return matchedNode;
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
    if (!(ch instanceof AvroClassHierarchy)) {
      throw new UnsupportedOperationException(
              "Cannot merge with class hierarchies of type: " + ch.getClass().getName());
    }

    final AvroClassHierarchy ach = (AvroClassHierarchy) ch;
    for (final String key : ach.lookupTable.keySet()) {
      if (!this.lookupTable.containsKey(key)) {
        this.lookupTable.put(key, ach.lookupTable.get(key));
      }
    }

    for (final Node n : ch.getNamespace().getChildren()) {
      if (!this.namespace.contains(n.getFullName())) {
        if (n instanceof NamedParameter) {
          final NamedParameterNode np = (NamedParameterNode) n;
          new NamedParameterNodeImpl<>(this.namespace, np.getName(), np.getFullName(), np.getFullArgName(),
                  np.getSimpleArgName(), np.isSet(), np.isList(), np.getDocumentation(), np.getShortName(),
                  np.getDefaultInstanceAsStrings());
        } else if (n instanceof ClassNode) {
          final ClassNode cn = (ClassNode) n;
          new ClassNodeImpl<>(namespace, cn.getName(), cn.getFullName(), cn.isUnit(), cn.isInjectionCandidate(),
                  cn.isExternalConstructor(), cn.getInjectableConstructors(), cn.getAllConstructors(),
                  cn.getDefaultImplementation());
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
