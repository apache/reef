using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;
using System.Collections.Generic;
using ProtoBuf;
using Com.Microsoft.Tang.Exceptions;
using System.IO;

namespace Com.Microsoft.Tang.Protobuf
{
    public class ProtocolBufferClassHierarchy
    {
        public static void Serialize(string fileName, IClassHierarchy classHierarchy)
        {
            ClassHierarchyProto.Node node = Serialize(classHierarchy);

            using (var file = File.Create(fileName)) 
            { 
                Serializer.Serialize<ClassHierarchyProto.Node>(file, node); 
            }
        }

        public static ClassHierarchyProto.Node Serialize(IClassHierarchy classHierarchy)
        {
            return SerializeNode(classHierarchy.GetNamespace());
        }

        private static ClassHierarchyProto.Node SerializeNode(INode n)
        {
            IList<ClassHierarchyProto.Node> children = new List<ClassHierarchyProto.Node>();

            foreach (INode child in n.GetChildren()) 
            {
                children.Add(SerializeNode(child));
            }


            if (n is IClassNode) 
            {
                IClassNode cn = (IClassNode) n;
                IList<IConstructorDef>injectable = cn.GetInjectableConstructors();
                IList<IConstructorDef> all = cn.GetAllConstructors();
                IList<IConstructorDef> others = new List<IConstructorDef>(all);

                foreach (var c  in injectable)
                {
                    others.Remove(c);
                }

                IList<ClassHierarchyProto.ConstructorDef> injectableConstructors = new List<ClassHierarchyProto.ConstructorDef>();
                foreach (IConstructorDef inj in injectable) 
                {
                    injectableConstructors.Add(SerializeConstructorDef(inj));
                }

                IList<ClassHierarchyProto.ConstructorDef> otherConstructors = new List<ClassHierarchyProto.ConstructorDef>();
                foreach (IConstructorDef other in others)
                {
                    otherConstructors.Add(SerializeConstructorDef(other));
                }
                
                List<string> implFullNames = new List<string>();
                foreach (IClassNode impl in cn.GetKnownImplementations()) 
                {
                    implFullNames.Add(impl.GetFullName());
                }

                return NewClassNode(cn.GetName(), cn.GetFullName(),
                    cn.IsInjectionCandidate(), cn.IsExternalConstructor(), cn.IsUnit(),
                    injectableConstructors, otherConstructors, implFullNames, children);

           
            } 
            else if (n is INamedParameterNode) 
            {
                INamedParameterNode np = (INamedParameterNode) n;
                return NewNamedParameterNode(np.GetName(), np.GetFullName(),
                    np.GetSimpleArgName(), np.GetFullArgName(), np.IsSet(), np.GetDocumentation(),
                    np.GetShortName(), np.GetDefaultInstanceAsStrings(), children);
            } 
            else if (n is IPackageNode) 
            {
                return NewPackageNode(n.GetName(), n.GetFullName(), children);
            } 
            else 
            {
                throw new IllegalStateException("Encountered unknown type of Node: " + n);
            }
        }

        private static ClassHierarchyProto.ConstructorDef SerializeConstructorDef(IConstructorDef def) 
        {
            IList<ClassHierarchyProto.ConstructorArg> args = new List<ClassHierarchyProto.ConstructorArg>();
            foreach (IConstructorArg arg in def.GetArgs())
            {
                args.Add(NewConstructorArg(arg.Gettype(), arg.GetNamedParameterName(), arg.IsInjectionFuture()));
            }
            return newConstructorDef(def.GetClassName(), args);
        }

        private static ClassHierarchyProto.ConstructorArg NewConstructorArg(
            string fullArgClassName, string namedParameterName, bool isFuture)
        {
            ClassHierarchyProto.ConstructorArg constArg = new ClassHierarchyProto.ConstructorArg();
            constArg.full_arg_class_name = fullArgClassName;
            constArg.named_parameter_name = namedParameterName;
            constArg.is_injection_future = isFuture;
            return constArg; 
        }

        private static ClassHierarchyProto.ConstructorDef newConstructorDef(
             String fullClassName, IList<ClassHierarchyProto.ConstructorArg> args)
        {
            ClassHierarchyProto.ConstructorDef constDef = new ClassHierarchyProto.ConstructorDef();
            constDef.full_class_name = fullClassName;
            foreach (ClassHierarchyProto.ConstructorArg arg in args)
            {
                constDef.args.Add(arg);
            }

            return constDef;
        }

        private static ClassHierarchyProto.Node NewClassNode(String name,
            String fullName, bool isInjectionCandidate,
            bool isExternalConstructor, bool isUnit,
            IList<ClassHierarchyProto.ConstructorDef> injectableConstructors,
            IList<ClassHierarchyProto.ConstructorDef> otherConstructors,
            IList<String> implFullNames, IList<ClassHierarchyProto.Node> children)
        {
            ClassHierarchyProto.ClassNode classNode = new ClassHierarchyProto.ClassNode();
            classNode.is_injection_candidate = isInjectionCandidate;
            foreach (var ic in injectableConstructors)
            {
                classNode.InjectableConstructors.Add(ic);
            }

            foreach (var oc in otherConstructors)
            {
                classNode.OtherConstructors.Add(oc);
            }
            foreach (var implFullName in implFullNames)
            {
                classNode.impl_full_names.Add(implFullName);
            }

            ClassHierarchyProto.Node n = new ClassHierarchyProto.Node();
            n.name = name;
            n.full_name = fullName;
            n.class_node = classNode;

            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }

        private static ClassHierarchyProto.Node NewNamedParameterNode(string name,
            string fullName, string simpleArgClassName, string fullArgClassName,
            bool isSet, string documentation, // can be null
            string shortName, // can be null
            string[] instanceDefault, // can be null
            IList<ClassHierarchyProto.Node> children)
        {
            ClassHierarchyProto.NamedParameterNode namedParameterNode = new ClassHierarchyProto.NamedParameterNode();
            namedParameterNode.simple_arg_class_name = simpleArgClassName;
            namedParameterNode.full_arg_class_name = fullArgClassName;
            namedParameterNode.is_set = isSet;

            if (documentation != null)
            {
                namedParameterNode.documentation = documentation;
            }

            if (shortName != null)
            {
                namedParameterNode.short_name = shortName;
            }

            foreach (var id in instanceDefault)
            {
                namedParameterNode.instance_default.Add(id);
            }

            ClassHierarchyProto.Node n = new ClassHierarchyProto.Node();
            n.name = name;
            n.full_name = fullName;
            n.named_parameter_node = namedParameterNode;

            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }

        private static ClassHierarchyProto.Node NewPackageNode(string name,
            string fullName, IList<ClassHierarchyProto.Node> children)
        {
            ClassHierarchyProto.PackageNode packageNode = new ClassHierarchyProto.PackageNode();
            ClassHierarchyProto.Node n = new ClassHierarchyProto.Node();
            n.name = name;
            n.full_name = fullName;
            n.package_node = packageNode;

            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }


        public static IClassHierarchy DeSerialize(string fileName)
        {
            ClassHierarchyProto.Node newNode;

            using (var file = File.OpenRead(fileName))
            {
                newNode = Serializer.Deserialize<ClassHierarchyProto.Node>(file);
            }

            return null;
        }
    }
}
