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
﻿using System;
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
using Com.Microsoft.Tang.Implementations;
using System.Collections;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Protobuf
{
    public class ProtocolBufferClassHierarchy : IClassHierarchy
    {
        private IPackageNode rootNode;

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
                IClassNode cn = (IClassNode)n;
                IList<IConstructorDef> injectable = cn.GetInjectableConstructors();
                IList<IConstructorDef> all = cn.GetAllConstructors();
                IList<IConstructorDef> others = new List<IConstructorDef>(all);

                foreach (var c in injectable)
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
                INamedParameterNode np = (INamedParameterNode)n;
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
            ClassHierarchyProto.Node root;

            using (var file = File.OpenRead(fileName))
            {
                root = Serializer.Deserialize<ClassHierarchyProto.Node>(file);
            }

            return new ProtocolBufferClassHierarchy(root);
        }

        public ProtocolBufferClassHierarchy(ClassHierarchyProto.Node root)
        {
            this.rootNode = new PackageNodeImpl();
            if (root.package_node == null)
            {
                throw new ArgumentException("Expected a package node.  Got: " + root);
            }
            // Register all the classes.
            foreach (ClassHierarchyProto.Node child in root.children)
            {
                ParseSubHierarchy(rootNode, child);
            }

            // Now, register the implementations
            foreach (ClassHierarchyProto.Node child in root.children)
            {
                WireUpInheritanceRelationships(child);
            }
        }

        private static void ParseSubHierarchy(INode parent, ClassHierarchyProto.Node n)
        {
            INode parsed;
            if (n.package_node != null)
            {
                parsed = new PackageNodeImpl(parent, n.name, n.full_name);
            }
            else if (n.named_parameter_node != null)
            {
                ClassHierarchyProto.NamedParameterNode np = n.named_parameter_node;
                parsed = new NamedParameterNodeImpl(parent, n.name,
                    n.full_name, np.full_arg_class_name, np.simple_arg_class_name,
                    np.is_set, np.documentation, np.short_name,
                    np.instance_default.ToArray());
            }
            else if (n.class_node != null)
            {
                ClassHierarchyProto.ClassNode cn = n.class_node;
                IList<IConstructorDef> injectableConstructors = new List<IConstructorDef>();
                IList<IConstructorDef> allConstructors = new List<IConstructorDef>();

                foreach (ClassHierarchyProto.ConstructorDef injectable in cn.InjectableConstructors)
                {
                    IConstructorDef def = ParseConstructorDef(injectable, true);
                    injectableConstructors.Add(def);
                    allConstructors.Add(def);
                }
                foreach (ClassHierarchyProto.ConstructorDef other in cn.OtherConstructors)
                {
                    IConstructorDef def = ParseConstructorDef(other, false);
                    allConstructors.Add(def);

                }

                IConstructorDef[] dummy = new ConstructorDefImpl[0];
                parsed = new ClassNodeImpl(parent, n.name, n.full_name,
                cn.is_unit, cn.is_injection_candidate,
                cn.is_external_constructor, injectableConstructors,
                allConstructors, cn.default_implementation);
            }

            else
            {
                throw new IllegalStateException("Bad protocol buffer: got abstract node" + n);
            }

            foreach (ClassHierarchyProto.Node child in n.children)
            {
                ParseSubHierarchy(parsed, child);
            }
        }

        private static IConstructorDef ParseConstructorDef(ClassHierarchyProto.ConstructorDef def, bool isInjectable)
        {
            IList<IConstructorArg> args = new List<IConstructorArg>();
            foreach (ClassHierarchyProto.ConstructorArg arg in def.args)
            {
                args.Add(new ConstructorArgImpl(arg.full_arg_class_name, arg.named_parameter_name, arg.is_injection_future));
            }
            return new ConstructorDefImpl(def.full_class_name, args.ToArray(), isInjectable);
        }

        private void WireUpInheritanceRelationships(ClassHierarchyProto.Node n)
        {
            if (n.class_node != null)
            {
                ClassHierarchyProto.ClassNode cn = n.class_node;
                IClassNode iface;
                try
                {
                    iface = (IClassNode)GetNode(n.full_name);
                }
                catch (NameResolutionException e)
                {
                    throw new IllegalStateException("When reading protocol buffer node "
                        + n.full_name + " does not exist.  Full record is " + n, e);
                }
                foreach (String impl in cn.impl_full_names)
                {
                    try
                    {
                        iface.PutImpl((IClassNode)GetNode(impl));
                    }
                    catch (NameResolutionException e)
                    {
                        throw new IllegalStateException("When reading protocol buffer node "
                            + n + " refers to non-existent implementation:" + impl);
                    }
                    catch (InvalidCastException e)
                    {
                        try
                        {
                            throw new IllegalStateException(
                                "When reading protocol buffer node " + n
                                + " found implementation" + GetNode(impl)
                                + " which is not a ClassNode!");
                        }
                        catch (NameResolutionException e2)
                        {
                            throw new IllegalStateException(
                                "Got 'cant happen' exception when producing error message for " + e);
                        }
                    }
                }
            }
        }

        public INode GetNode(String fullName)
        {       
            INode current = rootNode;
            string[] enclosingPath = ClassNameParser.GetEnclosingClassShortNames(fullName);
            for (int i = 0; i < enclosingPath.Length; i++)
            {
                current = current.Get(enclosingPath[i]);
                if (current == null)
                {
                    throw new NameResolutionException(fullName, enclosingPath[i]);
                }
            }
 
            return current;
        }


        private string[] GetEnclosingClassShortNames(string fullName)
        {
            string[] path = fullName.Split('+');

            if (path.Length == 1)
            {
               // path[0] = name;
            }
            string[] first = path[0].Split('.');
            path[0] = first[first.Length - 1];

            return path;
        }


        public INode GetNamespace()
        {
            return rootNode;
        }

        public bool IsImplementation(IClassNode inter, IClassNode impl)
        {
            return impl.IsImplementationOf(inter);
        }

        public IClassHierarchy Merge(IClassHierarchy ch)
        {
            if (this == ch)
            {
                return this;
            }
            throw new UnsupportedOperationException(
                "Cannot merge ExternalClassHierarchies yet!");
        }
    }
}
