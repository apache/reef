using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

using ClassHierarchy;

namespace Tang
{
    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class Unit : System.Attribute { }

    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class NamedParameter : System.Attribute
    {
        public string documentation;
        public Type argClass;
        public string defaultInstance;
        public string shortName;
        public NamedParameter(Type argClass, string documentation = "", string defaultInstance = "", string shortName = "")
        {
            this.argClass = argClass;
            this.documentation = documentation;
            this.defaultInstance = defaultInstance;
            this.shortName = shortName;
        }
    }
    
    [System.AttributeUsage(System.AttributeTargets.Constructor)]
    public class Inject : System.Attribute { }
    
    [System.AttributeUsage(System.AttributeTargets.Parameter)]
    public class Parameter : System.Attribute
    {
        public Name name;
    }
    public interface Name { }

    public class ClassHierarchy
    {
        Node rootNode = new Node();
        HashSet<KeyValuePair<String,String>> impls = new HashSet<KeyValuePair<String, String>>();
        public string prefix(string name, int n)
        {
            n++;
            for (int i = 0; i < name.Length; i++)
            {
                if (name.ElementAt(i) == '.')
                {
                    n--;
                }
                if (n == 0)
                {
                    return name.Substring(0, i);
                }
            }
            throw new IndexOutOfRangeException();
        }

        public Node Get(string name)
        {
            try
            {
                int i = 0;
                Node node = null;
                while (node == null)
                {
                    string pre = prefix(name, i);
                    node = rootNode.Children.Find(
                    delegate(Node n)
                    {
                        return n.FullName == pre;
                    }
                    );
                    i++;
                }
                string[] tok = name.Split(new char[] { '.' });
                for (int j = i; j < tok.Length; j++)
                {
                    node = node.Children.Find(
                    delegate(Node n)
                    {
                        return n.Name == tok[j];
                    }
                    );
                    if (node == null) { return null; }
                }
                return node;
            }
            catch (IndexOutOfRangeException e)
            {
                return null;
            }
        }
        public ClassHierarchy(String file)
        {
            Assembly assembly = Assembly.LoadFile(file);
            rootNode.PackageNode = new PackageNode();
            rootNode.Name = "";
            rootNode.FullName = "[C# root node]";
            foreach (var t in assembly.DefinedTypes) {
                rootNode.Children.Add(RegisterType(t));
            }
            foreach (var p in impls)
            {
                Get(p.Value).ClassNode.ImplFullNames.Add(p.Key);
            }
        }
        private Node RegisterType(TypeInfo t) {
            Node n = new Node();
            n.FullName = t.FullName;
            n.Name = t.Name;
            var isUnit = null != t.GetCustomAttribute<Unit>();
            var namedParameter = t.GetCustomAttribute<NamedParameter>();
            if (namedParameter != null)
            {
                n.NamedParameterNode = new NamedParameterNode();
                n.NamedParameterNode.Documentation = namedParameter.documentation;
                n.NamedParameterNode.FullArgClassName = namedParameter.argClass.FullName;
                n.NamedParameterNode.InstanceDefault = namedParameter.defaultInstance;
                n.NamedParameterNode.ShortName = namedParameter.shortName;
                n.NamedParameterNode.SimpleArgClassName = namedParameter.argClass.Name;
            }
            else
            {
                n.ClassNode = new ClassNode();
                n.ClassNode.IsExternalConstructor = false;
                n.ClassNode.IsUnit = isUnit;
                n.ClassNode.IsInjectionCandidate = true; // TODO: implement this!
                var superclass = t.BaseType;
                impls.Add(new KeyValuePair<string, string>(n.FullName, superclass.FullName));
                var interfaces = t.ImplementedInterfaces;
                foreach (var iface in interfaces)
                {
                    impls.Add(new KeyValuePair<string, string>(n.FullName, iface.FullName));
                }
                var injectableConstructors = new List<ConstructorDef>();
                var otherConstructors = new List<ConstructorDef>();
                foreach (var c in t.DeclaredConstructors)
                {
                    var isInjectable = null != c.GetCustomAttribute<Inject>();
                    ConstructorDef def = new ConstructorDef();
                    foreach (var p in c.GetParameters())
                    {
                        Parameter param = p.GetCustomAttribute<Parameter>();
                        ConstructorArg arg = new ConstructorArg();
                        arg.NamedParameterName = param.GetType().FullName;
                        // TODO: Handle default parameters somehow!
                        arg.FullArgClassName = p.ParameterType.FullName;
                        def.Args.Add(arg);
                    }
                    def.FullClassName = c.DeclaringType.FullName;
                    if (isInjectable)
                    {
                        n.ClassNode.InjectableConstructors.Add(def);
                    }
                    else
                    {
                        n.ClassNode.OtherConstructors.Add(def);
                    }
                }
                foreach (var m in t.DeclaredNestedTypes)
                {
                    n.Children.Add(RegisterType(m));
                }
            }
            return n;
        }
    }
}
