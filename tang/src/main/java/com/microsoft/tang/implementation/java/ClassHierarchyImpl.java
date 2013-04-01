package com.microsoft.tang.implementation.java;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ParameterParser;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class ClassHierarchyImpl implements ClassHierarchy {
  // TODO Want to add a "register namespace" method, but Java is not designed
  // to support such things.
  // There are third party libraries that would help, but they can fail if the
  // relevant jar has not yet been loaded.

  private final URLClassLoader loader;
  private final List<URL> jars;

  private final PackageNode namespace;
  private final TreeSet<String> registeredClasses = new MonotonicSet<>();
  // Note: this is only used to sanity check short names (so that name clashes get resolved).
  private final Map<String, NamedParameterNode<?>> shortNames = new MonotonicMap<>();

  public final ParameterParser parameterParser = new ParameterParser();

  // TODO: Fix up the exception handling surrounding parsing of values
  @Override
  public <T> T parseDefaultValue(NamedParameterNode<T> name) throws BindException {
    String val = name.getDefaultInstanceAsString();
    if (val != null) {
      return parse(name, val);
    } else {
      return null;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T parse(NamedParameterNode<T> name, String value) throws BindException {
    return (T) parse(name.getFullArgName(), value);
  }

  private Object parse(String name, String value) throws BindException {
    try {
      return parameterParser.parse(name, value);
    } catch (UnsupportedOperationException e) {
      try {
        return register(value);
      } catch (BindException e2) {
        throw new BindException("Could not parse type " + name + ".  Value was "
            + value, e2);
      }
    }
  }

  
  Class<?> classForName(String name) throws ClassNotFoundException {
    return ReflectionUtilities.classForName(name, loader);
  }

  public ClassHierarchyImpl(URL... jars) {
    this.namespace = JavaNodeFactory.createPackageNode();
    this.jars = new ArrayList<>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, this.getClass().getClassLoader());
  }
  private <T, U> Node buildPathToNode(Class<U> clazz)
      throws BindException {
    String[] path = clazz.getName().split(ReflectionUtilities.regexp);
    Node root = namespace;
    for (int i = 0; i < path.length - 1; i++) {
      root = root.get(path[i]);
    }

    if (root == null) {
      throw new NullPointerException();
    }
    Node parent = root;

    Class<?> argType = ReflectionUtilities.getNamedParameterTargetOrNull(clazz);

    if (argType == null) {
      return JavaNodeFactory.createClassNode(parent, clazz);
    } else {
      @SuppressWarnings("unchecked")
      // checked inside of NamedParameterNode, using reflection.
      NamedParameterNode<T> np = JavaNodeFactory.createNamedParameterNode(
          parent, (Class<? extends Name<T>>) clazz, (Class<T>) argType);
      String shortName = np.getShortName();
      if (shortName != null) {
        NamedParameterNode<?> oldNode = shortNames.get(shortName);
        if (oldNode != null) {
          if (oldNode.getFullName().equals(np.getFullName())) {
            throw new IllegalStateException("Tried to double bind "
                + oldNode.getFullName() + " to short name " + shortName);
          }
          throw new BindException("Named parameters " + oldNode.getFullName()
              + " and " + np.getFullName() + " have the same short name: "
              + shortName);
        }
        shortNames.put(shortName, np);
      }
      return np;
    }
  }

  private Node getNode(Class<?> clazz) throws NameResolutionException {
    return getAlreadyBoundNode(clazz.getName());
  }

  @Override
  public Node getNode(String name) throws NameResolutionException {
    try {
      register(name);
    } catch(BindException e) {
      throw new NameResolutionException(e);
    }
    return getAlreadyBoundNode(name);
  }
  private Node getAlreadyBoundNode(String name) throws NameResolutionException {
    String[] path = name.split(ReflectionUtilities.regexp);
    return getNode(name, path, path.length);
  }
  private Node getNode(String name, String[] path, int depth)
      throws NameResolutionException {
    Node root = namespace;
    for (int i = 0; i < depth; i++) {
      root = root.get(path[i]);
      if (root == null) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < i; j++) {
          sb.append(path[j]);
          if (j != i - 1) {
            sb.append(".");
          }
        }
        throw new NameResolutionException(name, sb.toString());
      }
    }
    return root;
  }

  private String arrayToDotString(String[] array, int length) {
    StringBuilder parentString = new StringBuilder(array[0]);
    for (int i = 1; i < length; i++) {
      parentString.append("." + array[i]);
    }
    return parentString.toString();
  }

  /**
   * Assumes parent packages are already registered.
   * 
   * @param packageName
   * @throws NameResolutionException
   */
  private void registerPackage(String[] packageName)
      throws NameResolutionException {

    try {
      getAlreadyBoundNode(arrayToDotString(packageName, packageName.length));
      return;
    } catch (NameResolutionException e) {
    }

    final PackageNode parent;
    if (packageName.length == 1) {
      parent = namespace;
    } else {
      parent = (PackageNode) getAlreadyBoundNode(arrayToDotString(packageName,
          packageName.length - 1));
    }
    JavaNodeFactory.createPackageNode(parent,
        packageName[packageName.length - 1]);
  }

  @Override
  public Node register(String s) throws BindException {
    final Class<?> c;
    try {
      c = classForName(s);
    } catch (ClassNotFoundException e1) {
      return null;
    }
    try {
      Node n = getNode(c);
      return n;
    } catch (NameResolutionException e) {
    }
    // First, walk up the class hierarchy, registering all out parents. This
    // can't be loopy.
    if (c.getSuperclass() != null) {
      register(ReflectionUtilities.getFullName(c.getSuperclass()));
    }
    for (Class<?> i : c.getInterfaces()) {
      register(ReflectionUtilities.getFullName(i));
    }
    // Now, we'd like to register our enclosing classes. This turns out to be
    // safe.
    // Thankfully, Java doesn't allow:
    // class A implements A.B { class B { } }

    // It also doesn't allow cycles such as:
    // class A implements B.BB { interface AA { } }
    // class B implements A.AA { interface BB { } }

    // So, even though grafting arbitrary DAGs together can give us cycles, Java
    // seems
    // to have our back on this one.
    Class<?> enclosing = c.getEnclosingClass();
    if (enclosing != null) {
      register(ReflectionUtilities.getFullName(enclosing));
    }
    Package pack = c.getPackage();
    if (pack != null) { // We're in an enclosing class, and we just registered
                        // it above!
      String[] packageList = pack.getName().split(ReflectionUtilities.regexp);
      for (int i = 0; i < packageList.length; i++) {
        try {
          registerPackage(Arrays.copyOf(packageList, i + 1));
        } catch (NameResolutionException e) {
          throw new IllegalStateException("Could not find parent package "
              + Arrays.toString(Arrays.copyOf(packageList, i + 1))
              + ", which this method should have registered.", e);
        }
      }
    }
    // Now, register the class. This has to be after the above so we know our
    // parents (superclasses and enclosing packages) are already registered.
    Node n = registerClass(c);

    // Finally, do things that might introduce cycles that invlove c.
    // This has to be below registerClass, which ensures that any cycles
    // this stuff introduces are broken.
    for (Class<?> inner_class : c.getDeclaredClasses()) {
      register(ReflectionUtilities.getFullName(inner_class));
    }
    if (n instanceof ClassNode) {
      ClassNode<?> cls = (ClassNode<?>) n;
      for (ConstructorDef<?> def : cls.getInjectableConstructors()) {
        for (ConstructorArg arg : def.getArgs()) {
          register(arg.getType());
          if (arg.getNamedParameterName() != null) {
            NamedParameterNode<?> np = (NamedParameterNode<?>) register(arg
                .getNamedParameterName());
            try {
              if (!ReflectionUtilities.isCoercable(classForName(arg.getType()),
                  classForName(np.getFullArgName()))) {
                throw new BindException(
                    "Incompatible argument type.  Constructor expects "
                        + arg.getType() + " but " + np.getName() + " is a "
                        + np.getFullArgName());
              }
            } catch (ClassNotFoundException e) {
              throw new BindException("Constructor refers to unknown class "
                  + arg.getType(), e);
            }
          }
        }
      }
    } else if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      register(np.getFullArgName());
    }
    return n;
  }

  /**
   * Assumes that all of the parents of c have been registered already.
   * 
   * @param c
   */
  @SuppressWarnings("unchecked")
  private <T, U extends T> Node registerClass(final Class<U> c)
      throws BindException {
    if (c.isArray()) {
      throw new BindException("Can't register array types");
    }
    try {
      return getNode(c);
    } catch (NameResolutionException e) {
    }

    final Node n = buildPathToNode(c);

    if (n instanceof ClassNode) {
      ClassNode<U> cn = (ClassNode<U>) n;
      Class<T> superclass = (Class<T>) c.getSuperclass();
      if (superclass != null) {
        try {
          ((ClassNode<T>) getNode(superclass)).putImpl(cn);
        } catch (NameResolutionException e) {
          throw new IllegalStateException(e);
        }
      }
      for (Class<?> interf : c.getInterfaces()) {
        try {
          ((ClassNode<T>) getNode(interf)).putImpl(cn);
        } catch (NameResolutionException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    registeredClasses.add(ReflectionUtilities.getFullName(c));
    return n;
  }

  public Set<String> getRegisteredClassNames() {
    return new MonotonicSet<String>(registeredClasses);
  }

  PackageNode getNamespace() {
    return namespace;
  }

  @Override
  public String toPrettyString() {
    return namespace.toIndentedString(0);
  }

  @Override
  public boolean isImplementation(ClassNode<?> inter, ClassNode<?> impl) {
    List<ClassNode<?>> worklist = new ArrayList<>();
    if (impl.equals(inter)) {
      return true;
    }
    worklist.add(inter);
    while (!worklist.isEmpty()) {
      ClassNode<?> cn = worklist.remove(worklist.size() - 1);
      @SuppressWarnings({ "rawtypes", "unchecked" })
      Set<ClassNode<?>> impls = (Set) cn.getKnownImplementations();
      if (impls.contains(impl)) {
        return true;
      }
      worklist.addAll(impls);
    }
    return false;
  }

  @Override
  public ClassHierarchy merge(ClassHierarchy ch) {
    if(this == ch) { return this; }
    if(!(ch instanceof ClassHierarchyImpl)) {
      throw new UnsupportedOperationException("Can't merge java and non-java class hierarchies yet!");
    }
    if(this.jars.size() == 0) {
      return ch;
    }
    ClassHierarchyImpl chi = (ClassHierarchyImpl)ch;
    HashSet<URL> otherJars = new HashSet<>();
    otherJars.addAll(chi.jars);
    HashSet<URL> myJars = new HashSet<>();
    myJars.addAll(this.jars);
    if(myJars.containsAll(otherJars)) {
      return this;
    } else if(otherJars.containsAll(myJars)) {
      return ch;
    } else {
      myJars.addAll(otherJars);
      return new ClassHierarchyImpl(myJars.toArray(new URL[0]));
    }
    
    
  }
}
