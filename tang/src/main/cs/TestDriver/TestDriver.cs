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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;

namespace Com.Microsoft.Tang.TestDriver
{
    public class TestDriver
    {
        static string file = @"Com.Microsoft.Tang.Examples";
        static string file2 = @"com.microsoft.reef.activity";
        static string file3 = @"com.microsoft.reef.activityInterface";

        public static void Main(string[] args)
        {
            ActivityTest();
            FileTest();
            TypeTest();
            CreateClassHierarchy();
        }

        public static void ActivityTest()
        {
            var a = (com.microsoft.reef.activity.IActivity)TangFactory.GetTang()
            .NewInjector(new string[] { @"com.microsoft.reef.activity", @"com.microsoft.reef.ActivityInterface" }, "activityConf1.txt")
            .GetInstance(typeof(com.microsoft.reef.activity.IActivity));
            a.Call(null);
        }

        public static void CreateClassHierarchy()
        {
            ClassHierarchyImpl classHierarchyImpl = new ClassHierarchyImpl(file);
            GetNodeFromHierarchy(classHierarchyImpl);
        }

        public static void GetNodeFromHierarchy(ClassHierarchyImpl classHierarchyImpl)
        {
            IClassNode timerClassNode = (IClassNode)classHierarchyImpl.GetNode("Com.Microsoft.Tang.Examples.Timer");
            INode secondNode = classHierarchyImpl.GetNode("Com.Microsoft.Tang.Examples.Timer+Seconds");

            string classNmae = timerClassNode.GetFullName();
            //Type clazz = classHierarchyImpl.loader.GetType(classNmae);

            IList<IConstructorDef> constuctorDefs = timerClassNode.GetAllConstructors();
            foreach (IConstructorDef consDef in constuctorDefs)
            {
                IList<IConstructorArg> consArgs = consDef.GetArgs();
                foreach (IConstructorArg arg in consArgs)
                {
                    string argName = arg.GetName();
                    string argTypeName = arg.Gettype();
                    Type nt = Type.GetType(argName);
                    INode argNode = classHierarchyImpl.GetNode(nt);
                }
            }
        }

        public static void FileTest()
        {
            WriteLine("testfile1.txt");
            WriteToString("testfile2.txt");

            ReadFromFile("testfile1.txt");
            ReadFromFile("testfile2.txt");
        }

        private static void ReadFromFile(string fileName)
        {
            IDictionary<string, string> property = new Dictionary<string, string>();
            using (StreamReader sr = new StreamReader(fileName))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    string[] p = line.Split('=');
                    property.Add(p[0], p[1]);
                }
            }
        }

        private static void WriteToString(string fileName)
        {
            List<string> l = new List<string>();
            l.Add("key1=value1");
            l.Add("key2=value2");
            l.Add("key3=value3");
            l.Add("key4=value4");

            StringBuilder sb = new StringBuilder();
            foreach (string s in l)
            {
                sb.Append(s);
                sb.Append('\n');
            }

            FileStream aFile2 = new FileStream(fileName, FileMode.OpenOrCreate);
            using (StreamWriter sw = new StreamWriter(aFile2))
            {
                sw.Write(sb.ToString());
            }
        }

        private static void WriteLine(string fileName)
        {
            FileStream aFile = new FileStream(fileName, FileMode.OpenOrCreate);
            using (StreamWriter sw = new StreamWriter(aFile))
            {

                sw.WriteLine("key1=value1");
                sw.WriteLine("key2=value2");
                sw.WriteLine("key3=value3");
                sw.WriteLine("key4=value4");
            }
        }
        public static void TypeTest()
        {
            var asm = Assembly.Load(@"Com.Microsoft.Tang.Examples");
            var types = asm.GetTypes();

            //Type type = Type.GetType(@"Com.Microsoft.Tang.Examples.Timer+Seconds");

            //Unit testing
            //Test test = new Test();
            //test.Add(4);
            //test.Add("avc");

            //GetRawType testing
            //Generic<int> gtObj = new Generic<int>();
            //Type gt = gtObj.GetType();
            //bool isG = gt.IsGenericType;
            //Type gtd = gt.GetGenericTypeDefinition();

            //IList<int> gtObj1 = new List<int>();
            //Type gt1 = gtObj1.GetType();
            //bool isG1 = gt1.IsGenericType;
            //Type gtd1 = gt1.GetGenericTypeDefinition();


            foreach (Type t in types)
            {
                System.Console.WriteLine("Full name of the type: " + t.FullName);
                System.Console.WriteLine("name of the type: " + t.Name);
                System.Console.WriteLine("AssemblyQualifiedName of the type: " + t.AssemblyQualifiedName);

                Type taa = Type.GetType(t.AssemblyQualifiedName);
                System.Console.WriteLine("Type from name " + taa.FullName);

                var unit = t.GetCustomAttribute<UnitAttribute>();
                var namedParameter = t.GetCustomAttribute<NamedParameterAttribute>();

                if (t.IsAbstract && t.IsSealed)
                    System.Console.WriteLine("this is a static" + t.FullName);


                Type[] intfs = t.GetInterfaces();
                foreach (Type f in intfs)
                {
                    System.Console.WriteLine("Interface: " + f.Name);
                    System.Console.WriteLine("Interface: " + f.FullName);

                    if (f.Name.Equals("Name`1"))
                    {
                        System.Console.WriteLine("The class extend from Name");
                    }
                    foreach (var a in f.GetGenericArguments())
                    {
                        System.Console.WriteLine("GetGenericArguments " + a.Name);

                    }
                    ConstructorDetails(f);
                }

                System.Console.WriteLine("Constructore details");
                ConstructorDetails(t);

                Type baseT = t.BaseType;
                if (baseT != null)
                {
                    System.Console.WriteLine("base type " + baseT.FullName);
                }

                Type[] inners = t.GetNestedTypes();
                foreach (Type inn in inners)
                {
                    System.Console.WriteLine("inner class " + inn.FullName);
                }

                //get name psace
                string[] namesaces = t.FullName.Split('.');
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < namesaces.Length - 1; i++)
                {
                    sb.Append(namesaces[i]);
                    sb.Append(".");
                }
                string namesp = sb.ToString();
                System.Console.WriteLine("namespace " + namesp);

                //get enclosing classes full name
                string[] path = t.FullName.Split('+');
                for (int i = 1; i < path.Length; i++)
                {
                    path[i] = path[i - 1] + "+" + path[i];
                }

                foreach (string s in path)
                    System.Console.WriteLine("eclosing class: " + s);

                if (namedParameter != null)
                {
                    System.Console.WriteLine(namedParameter);
                    System.Console.WriteLine(namedParameter.ShortName);
                    System.Console.WriteLine(namedParameter.DefaultValue);
                }
                System.Console.WriteLine("<<<<<<");
            }


            Type t3 = Type.GetType(typeof(Com.Microsoft.Tang.TestDriver.TestDriver).AssemblyQualifiedName);

            var tn = typeof(Name<int>);
            var tnName = tn.Name;


            Type t4 = typeof(Com.Microsoft.Tang.Examples.B);
            var t41 = t4.AssemblyQualifiedName;
            var t42 = t4.BaseType.AssemblyQualifiedName;
            System.Console.WriteLine("t4 " + t4);

            Type t411 = Type.GetType(t41);
            System.Console.WriteLine("t411 " + t411.FullName);

            Type t412 = Type.GetType(t42);
            System.Console.WriteLine("t412 " + t412.FullName);

            Type t5 = Type.GetType("Com.Microsoft.Tang.TestDriver.TestDriver");
            System.Console.WriteLine("t5 " + t5.FullName);

            Type t6 = asm.GetType("Com.Microsoft.Tang.Examples.A");
            System.Console.WriteLine("t6 " + t6);

            foreach (Type t in types)
            {
                if (t.IsInterface)
                {
                    System.Console.WriteLine(t.FullName + " is an interface: ");
                }

                if (t.IsClass)
                {
                    Type b1 = t.BaseType;
                    Type[] infcs = t.GetInterfaces();
                    System.Console.WriteLine(t.FullName + " is a class, and its base type is : " + b1.FullName);

                    foreach (Type t2 in infcs)
                    {
                        System.Console.WriteLine(t.FullName + " is a class, and its interface  is : " + t2.FullName);
                    }
                }
            }
            System.Console.WriteLine("hello");
        }

        private static void ConstructorDetails(Type t)
        {
            var consttr = t.GetConstructors();
            foreach (ConstructorInfo ci in consttr)
            {
                System.Console.WriteLine("constructor info " + ci.ToString());
                if (ci.ContainsGenericParameters)
                {
                    var genericArg = ci.GetGenericArguments();
                    System.Console.WriteLine("Constructor genericArg : " + genericArg.Length);
                }

                foreach (var pm in ci.GetParameters())
                {
                    Type type = pm.ParameterType;

                    ParameterAttribute named = pm.GetCustomAttribute<ParameterAttribute>();
                    if (named != null)
                    {
                        System.Console.WriteLine("Named Parameter name " + named.Value.AssemblyQualifiedName);
                        Type nt = Type.GetType(named.Value.AssemblyQualifiedName);
                        System.Console.WriteLine("Named Parameter Type " + nt.FullName);

                    }
                    System.Console.WriteLine("constructor param " + pm.Name);
                }
            }
        }
    }
 }
