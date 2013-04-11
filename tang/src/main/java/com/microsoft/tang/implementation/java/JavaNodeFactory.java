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
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.implementation.types.ClassNodeImpl;
import com.microsoft.tang.implementation.types.ConstructorArgImpl;
import com.microsoft.tang.implementation.types.ConstructorDefImpl;
import com.microsoft.tang.implementation.types.NamedParameterNodeImpl;
import com.microsoft.tang.implementation.types.PackageNodeImpl;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class JavaNodeFactory {

  @SuppressWarnings("unchecked")
  static <T> ClassNodeImpl<T> createClassNode(Node parent, Class<T> clazz) throws ClassHierarchyException {
    final boolean injectable;
    final boolean unit = clazz.isAnnotationPresent(Unit.class);
    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);
    final boolean parentIsUnit = (parent instanceof ClassNode) ?
        ((ClassNode<?>)parent).isUnit() : false;
        
    if (clazz.isLocalClass() || clazz.isMemberClass()) {
      if (!Modifier.isStatic(clazz.getModifiers())) {
        if(parent instanceof ClassNode) {
          injectable = ((ClassNode<?>)parent).isUnit();
        } else {
          injectable = false;
        }
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
      boolean constructorAnnotatedInjectable = (constructors[k]
          .getAnnotation(Inject.class) != null);
      if (constructorAnnotatedInjectable && constructors[k].isSynthetic()) {
        // Not sure if we *can* unit test this one.
        throw new ClassHierarchyException(
            "Synthetic constructor was annotated with @Inject!");
      }
      if (parentIsUnit && (constructorAnnotatedInjectable || constructors[k].getParameterTypes().length != 1)) {
        throw new ClassHierarchyException(
            "Detected explicit constructor in class enclosed in @Unit " + fullName + "  Such constructors are disallowed.");
      }
      boolean constructorInjectable = constructorAnnotatedInjectable || parentIsUnit;
      // ConstructorDef's constructor checks for duplicate
      // parameters
      // The injectableConstructors set checks for ambiguous
      // boundConstructors.
      ConstructorDef<T> def = JavaNodeFactory.createConstructorDef(injectable,
          constructors[k], constructorAnnotatedInjectable);
      if (constructorInjectable) {
        if (injectableConstructors.contains(def)) {
          throw new ClassHierarchyException(
              "Ambiguous boundConstructors detected in class " + clazz + ": "
                  + def + " differs from some other " + " constructor only "
                  + "by parameter order.");
        } else {
          injectableConstructors.add(def);
        }
      }
      allConstructors.add(def);
    }

    return new ClassNodeImpl<T>(parent, simpleName, fullName, unit, injectable,
        ExternalConstructor.class.isAssignableFrom(clazz),
        injectableConstructors.toArray(new ConstructorDefImpl[0]),
        allConstructors.toArray(new ConstructorDefImpl[0]));
  }

  public static <T> NamedParameterNode<T> createNamedParameterNode(Node parent,
      Class<? extends Name<T>> clazz, Class<T> argClass) throws ClassHierarchyException {
    final String simpleName = ReflectionUtilities.getSimpleName(clazz);
    final String fullName = ReflectionUtilities.getFullName(clazz);
    NamedParameter namedParameter = clazz.getAnnotation(NamedParameter.class);
    final String fullArgName = ReflectionUtilities.getFullName(argClass);
    final String simpleArgName = ReflectionUtilities.getSimpleName(argClass);
    final String defaultInstanceAsString;
    if (namedParameter == null || namedParameter.default_value().length() == 0) {
      defaultInstanceAsString = null;
    } else {
      defaultInstanceAsString = namedParameter.default_value();
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

  public static PackageNode createPackageNode() {
    return new PackageNodeImpl(null, "", "[root node]");
  }

  private static <T> ConstructorDef<T> createConstructorDef(
      boolean isClassInjectionCandidate, Constructor<T> constructor,
      boolean injectable) throws ClassHierarchyException {
    // We don't support injection of non-static member classes with @Inject
    // annotations.
    if (injectable && !isClassInjectionCandidate) {
      throw new ClassHierarchyException("Cannot @Inject non-static member class unless the enclosing class an @Unit.  Nested class is:"
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
//    try {
      return new ConstructorDefImpl<T>(
          ReflectionUtilities.getFullName(constructor.getDeclaringClass()),
          args, injectable);
//    } catch (ClassHierarchyException e) {
//      throw new ClassHierarchyException("Detected bad constructor in " + constructor
//          + " in "
//          + ReflectionUtilities.getFullName(constructor.getDeclaringClass()), e);
//    }
  }

}
