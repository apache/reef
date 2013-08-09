package com.microsoft.tang.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.util.walk.AbstractClassHierarchyNodeVisitor;
import com.microsoft.tang.util.walk.NodeVisitor;
import com.microsoft.tang.util.walk.Walk;

public class Tint {
  final JavaClassHierarchy ch;
  
  public Tint() {
    this(new URL[0]);
  }
  public Tint(URL[] jars) {
    this(jars, false);
  }
  public Tint(URL[] jars, boolean checkTang) {
    Object[] args = new Object[jars.length + 6];
    for(int i = 0; i < jars.length; i++){
      args[i] = jars[i];
    }
    args[args.length-1] = new TypeAnnotationsScanner();
    args[args.length-2] = new SubTypesScanner();
    args[args.length-3] = new MethodAnnotationsScanner();
    args[args.length-4] = new MethodParameterScanner();
    args[args.length-5] = "com.microsoft";
    args[args.length-6] = "org.apache";
    Reflections r = new Reflections(args);
    Set<Class<?>> classes = new HashSet<>();
    Set<String> strings = new TreeSet<>();

    // Workaround bug in Reflections by keeping things stringly typed, and using Tang to parse them.
//  Set<Constructor<?>> injectConstructors = (Set<Constructor<?>>)(Set)r.getMethodsAnnotatedWith(Inject.class);
//  for(Constructor<?> c : injectConstructors) {
//    classes.add(c.getDeclaringClass());
//  }
    Set<String> injectConstructors = r.getStore().getConstructorsAnnotatedWith(ReflectionUtilities.getFullName(Inject.class));
    for(String s : injectConstructors) {
      strings.add(s.replaceAll("\\.<.+$",""));
    }
    Set<String> parameterConstructors = r.getStore().get(MethodParameterScanner.class, ReflectionUtilities.getFullName(Parameter.class));
    for(String s : parameterConstructors) {
      strings.add(s.replaceAll("\\.<.+$",""));
    }
//    Set<Class> r.getConstructorsWithAnyParamAnnotated(Parameter.class);
//    for(Constructor<?> c : parameterConstructors) {
//      classes.add(c.getDeclaringClass());
//    }
    strings.addAll(r.getStore().get(TypeAnnotationsScanner.class, ReflectionUtilities.getFullName(DefaultImplementation.class)));
    strings.addAll(r.getStore().get(TypeAnnotationsScanner.class, ReflectionUtilities.getFullName(NamedParameter.class)));
    strings.addAll(r.getStore().get(TypeAnnotationsScanner.class, ReflectionUtilities.getFullName(Unit.class)));
//    classes.addAll(r.getTypesAnnotatedWith(DefaultImplementation.class));
//    classes.addAll(r.getTypesAnnotatedWith(NamedParameter.class));
//    classes.addAll(r.getTypesAnnotatedWith(Unit.class));
    
    strings.addAll(r.getStore().get(SubTypesScanner.class, ReflectionUtilities.getFullName(Name.class)));
//    classes.addAll(r.getSubTypesOf(Name.class));
    
    ch = Tang.Factory.getTang().getDefaultClassHierarchy(jars, new Class[0]);
    for(Class<?> c : classes) {
      strings.add(ReflectionUtilities.getFullName(c));
    }
    for(String s : strings) {
      try {
        // Tang's unit tests include a lot of nasty corner cases; no need to look at those!
        if(checkTang || !s.startsWith("com.microsoft.tang")) {
//          System.out.println("Scanning " + s);
          ch.getNode(s);
        }
      } catch(ClassHierarchyException | NameResolutionException e) {
        System.err.println(e.getMessage());
      }
    }
  }
  
  public Set<NamedParameterNode<?>> getNames() {
    final Set<NamedParameterNode<?>> names = new MonotonicSet<>();
    NodeVisitor<Node> v = new AbstractClassHierarchyNodeVisitor() {
      
      @Override
      public boolean visit(NamedParameterNode<?> node) {
        names.add(node);
        return true;
      }
      
      @Override
      public boolean visit(PackageNode node) {
        return true;
      }
      
      @Override
      public boolean visit(ClassNode<?> node) {
        return true;
      }
    };
    Walk.preorder(v, null, ch.getNamespace());
    return names;
  }
  private Set<Node> getUsagesOf(final NamedParameterNode name) {
    final Set<Node> usages= new MonotonicSet<>();
    NodeVisitor<Node> v = new AbstractClassHierarchyNodeVisitor() {
      
      @Override
      public boolean visit(NamedParameterNode<?> node) {
        if(name.getFullName().equals(node.getDefaultInstanceAsString())) {
          usages.add(node);
        }
        return true;
      }
      
      @Override
      public boolean visit(PackageNode node) {
        return true;
      }
      
      @Override
      public boolean visit(ClassNode<?> node) {
        for(ConstructorDef<?> d : node.getInjectableConstructors()) {
          for(ConstructorArg a : d.getArgs()) {
            if(name.getFullName().equals(a.getNamedParameterName())) {
              usages.add(node);
              return true;
            }
          }
        }
        return true;
      }
    };
    Walk.preorder(v, null, ch.getNamespace());
    return usages;
  }
  public String stripCommonPrefixes(String s) {
    return 
        stripPrefixHelper(
        stripPrefixHelper(s, "java.lang"),
        "java.util");
  }
  public String stripPrefixHelper(String s, String prefix) {
    if(s.startsWith(prefix)) {
      return s.substring(prefix.length()+1);
    } else {
      return s;
    }
  }
  public String stripPrefix(String s, String prefix) {
    return stripCommonPrefixes(stripPrefixHelper(s,prefix));
  }
  public String toString(NamedParameterNode n) {
    StringBuilder sb = new StringBuilder("Name: " + n.getSimpleArgName() + " " + n.getFullName());
    if(n.getDefaultInstanceAsString() != null) {
      sb.append(" = " + n.getDefaultInstanceAsString());
    }
    if(!n.getDocumentation().equals("")) {
      sb.append(" //" + n.getDocumentation());
    }
    return sb.toString();
  }
  public String toHtmlString(NamedParameterNode n, String pack) {
    final String sep = "</td><td>";
    String fullName = stripPrefix(n.getFullName(), pack);

    StringBuilder sb = new StringBuilder("<tr><td>" + n.getSimpleArgName() + sep + fullName + sep);
    if(n.getDefaultInstanceAsString() != null) {
      String instance = stripPrefix(n.getDefaultInstanceAsString(), pack);
      
      sb.append(instance);
    }
    if(!n.getDocumentation().equals("")) {
      sb.append(sep + n.getDocumentation());
    }
    Set<Node> uses = getUsagesOf(n);
    sb.append(sep);
    for(Node u : uses) {
      sb.append(stripPrefix(u.getFullName(), pack) + "<br>");
    }
    sb.append("</td></tr>");
    return sb.toString();
  }
  public String startPackage(String pack) {
    return "<tr><td colspan='4'><br><b>" + pack + "</b></td></tr>";
  }
  public String endPackage() {
    return "";
  }
  
  /**
   * @param args
   * @throws FileNotFoundException 
   * @throws MalformedURLException 
   */
  public static void main(String[] args) throws FileNotFoundException, MalformedURLException {
    int i = 0;
    String doc = null;
    String jar = null;
    boolean tangTests = false;
    while(i < args.length) {
      if(args[i].equals("--doc")) {
        i++;
        doc = args[i];
      } else if(args[i].equals("--jar")) {
        i++;
        jar = args[i];
      } else if(args[i].equals("--tang-tests")) {
        tangTests = true;
      }
      
      i++;
    }
    final Tint t;
    if(jar != null) {
      File f = new File(jar);
      if(!f.exists()) { throw new FileNotFoundException(jar); }
      t = new Tint(new URL[] { f.toURI().toURL() }, tangTests);
    } else {
      t = new Tint(new URL[0], tangTests);
    }
    if(doc != null) {
      PrintStream out = new PrintStream(new FileOutputStream(new File(doc)));
      out.println("<html><head><title>TangDoc</title></head><body>");
      out.println("<table><tr><th>Type</th><th>Name</th><th>Default value</th><th>Documentation</th><th>Used by</th></tr>");

      String currentPackage = "";
      for(NamedParameterNode<?> n : t.getNames()) {
        String fullName = n.getFullName();
        String tok[] = fullName.split("\\.");
        StringBuffer sb = new StringBuffer(tok[0]);
        for(int j = 1; j < tok.length; j++) {
          if(tok[j].matches("^[A-Z].+") || j > 4) {
            break;
          } else
            sb.append("." + tok[j]);
        }
        String pack = sb.toString();
        if(!currentPackage.equals(pack)) {
          currentPackage = pack;
          out.println(t.endPackage());
          out.println(t.startPackage(currentPackage));
          
        }
        out.println(t.toHtmlString(n, currentPackage));
      }
      out.println(t.endPackage());
      out.println("</table>");
      out.println("</body></html>");
      out.close();
      
    }
  }

}
