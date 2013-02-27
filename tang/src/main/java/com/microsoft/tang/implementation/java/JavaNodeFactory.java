package com.microsoft.tang.implementation.java;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;

import javax.inject.Inject;

import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.ClassNodeImpl;
import com.microsoft.tang.implementation.ConstructorArgImpl;
import com.microsoft.tang.implementation.ConstructorDefImpl;
import com.microsoft.tang.implementation.NamedParameterNodeImpl;
import com.microsoft.tang.implementation.NamespaceNodeImpl;
import com.microsoft.tang.implementation.PackageNodeImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.NamespaceNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class JavaNodeFactory {

  @SuppressWarnings("unchecked")
  static <T> ClassNodeImpl<T> createClassNode(Node parent, Class<T> clazz,
      boolean isPrefixTarget) throws BindException {
    // super(parent, ReflectionUtilities.getSimpleName(clazz));
    final boolean injectable;
    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);

    if (clazz.isLocalClass() || clazz.isMemberClass()) {
      if (!Modifier.isStatic(clazz.getModifiers())) {
        injectable = false;
      } else {
        injectable = true;
      }
    } else {
      injectable = true;
    }

    Constructor<T>[] constructors = (Constructor<T>[]) clazz
        .getDeclaredConstructors();
    MonotonicSet<ConstructorDef<T>> injectableConstructors = new MonotonicSet<>();
    ArrayList<ConstructorDef<T>> allConstructors = new ArrayList<>();
    for (int k = 0; k < constructors.length; k++) {
      boolean constructorIsInjectable = (constructors[k]
          .getAnnotation(Inject.class) != null);
      if (constructorIsInjectable && constructors[k].isSynthetic()) {
        // Not sure if we *can* unit test this one.
        throw new IllegalStateException(
            "Synthetic constructor was annotated with @Inject!");
      }

      // ConstructorDef's constructor checks for duplicate
      // parameters
      // The injectableConstructors set checks for ambiguous
      // boundConstructors.
      ConstructorDef<T> def = JavaNodeFactory.createConstructorDef(injectable,
          constructors[k], constructorIsInjectable);
      if (constructorIsInjectable) {
        if (injectableConstructors.contains(def)) {
          throw new BindException(
              "Ambiguous boundConstructors detected in class " + clazz + ": "
                  + def + " differs from some other " + " constructor only "
                  + "by parameter order.");
        } else {
          injectableConstructors.add(def);
        }
      }
      allConstructors.add(def);
    }

    return new ClassNodeImpl<T>(parent, simpleName, fullName, injectable,
        ExternalConstructor.class.isAssignableFrom(clazz),
        isPrefixTarget,
        injectableConstructors.toArray(new ConstructorDefImpl[0]),
        allConstructors.toArray(new ConstructorDefImpl[0]));
  }

  public static <T> NamedParameterNode<T> createNamedParameterNode(Node parent,
      Class<? extends Name<T>> clazz, Class<T> argClass) throws BindException {
    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);
    NamedParameter namedParameter = clazz.getAnnotation(NamedParameter.class);
    final String fullArgName = ReflectionUtilities.getFullName(argClass);
    final String simpleArgName = ReflectionUtilities.getSimpleName(argClass);
    final String defaultInstanceAsString;
    if (namedParameter == null || namedParameter.default_value().length() == 0) {
      defaultInstanceAsString = null;
    } else {
      try {
        defaultInstanceAsString = namedParameter.default_value();
      } catch (UnsupportedOperationException e) {
        throw new BindException("Could not register NamedParameterNode for "
            + clazz.getName() + ".  Default value "
            + namedParameter.default_value() + " failed to parse.", e);
      }
    }
    final String documentation;
    final String shortName;
    if (namedParameter != null) {
      documentation = namedParameter.doc();
      if (namedParameter.short_name() != null
          && namedParameter.short_name().length() == 0) {
        shortName = null;
      } else {
        shortName = namedParameter.short_name();
      }
    } else {
      documentation = "";
      shortName = null;
    }
    return new NamedParameterNodeImpl<>(parent, simpleName, fullName,
        fullArgName, simpleArgName, documentation, shortName,
        defaultInstanceAsString);
  }

  public static <T> NamespaceNode<T> createNamespaceNode(Node root,
      String name, ClassNode<T> target) {
    return new NamespaceNodeImpl<>(root, name, target);
  }

  public static NamespaceNode<?> createNamespaceNode(Node root, String name) {
    return new NamespaceNodeImpl<>(root, name);
  }

  public static PackageNode createPackageNode() {
    return new PackageNodeImpl(null, "");
  }

  public static PackageNode createPackageNode(Node parent, String name) {
    return new PackageNodeImpl(parent, name);
  }

  private static <T> ConstructorDef<T> createConstructorDef(
      boolean isClassInjectionCandidate, Constructor<T> constructor,
      boolean injectable) throws BindException {
    // We don't support injection of non-static member classes with @Inject
    // annotations.
    if (injectable && !isClassInjectionCandidate) {
      throw new BindException("Cannot @Inject non-static member/local class: "
          + ReflectionUtilities.getFullName(constructor.getDeclaringClass()));
    }
    Class<?>[] paramTypes = constructor.getParameterTypes();
    Annotation[][] paramAnnotations = constructor.getParameterAnnotations();
    if (paramTypes.length != paramAnnotations.length) {
      throw new IllegalStateException();
    }
    ConstructorArg[] args = new ConstructorArg[paramTypes.length];
    for (int i = 0; i < paramTypes.length; i++) {
      // if there is an appropriate annotation, use that.
      Parameter named = null;
      for (int j = 0; j < paramAnnotations[i].length; j++) {
        Annotation annotation = paramAnnotations[i][j];
        if (annotation instanceof Parameter) {
          named = (Parameter) annotation;
        }
      }
      args[i] = new ConstructorArgImpl(
          ReflectionUtilities.getFullName(paramTypes[i]), named == null ? null
              : ReflectionUtilities.getFullName(named.value()));
    }
    try {
      return new ConstructorDefImpl<T>(
          ReflectionUtilities.getFullName(constructor.getDeclaringClass()),
          args, injectable);
    } catch (BindException e) {
      throw new BindException("Detected bad constructor in " + constructor
          + " in "
          + ReflectionUtilities.getFullName(constructor.getDeclaringClass()), e);
    }
  }

}
