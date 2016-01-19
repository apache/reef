// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Examples.Tasks.ShellTask;
using Org.Apache.REEF.Examples.Tasks.StreamingTasks;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Protobuf;
using Org.Apache.REEF.Tang.Types;

namespace Org.Apache.REEF.Tang.Tools
{
    public class ClassHierarchyBuilder
    {
        /// <summary>
        /// This program generates class hierarchy bin file for the list of dlls, plus a defalut list
        /// The default list include: ITask, StreamTask1, HelloTask and ShellTask, please remove if not needed
        /// </summary>
        /// <param name="args"> additional dlls needed to build class hierarchy </param>
        public static void Main(string[] args)
        {
            const string DllSubfix = ".dll";
            const string ClassHierarchyBinFileName = "task.bin";

            List<string> taskDlls = new List<string>();

            foreach (string arg in args)
            {
                string assemblyName = arg;
                if (!arg.EndsWith(DllSubfix, StringComparison.OrdinalIgnoreCase))
                {
                    assemblyName += DllSubfix;
                }
                if (!File.Exists(assemblyName))
                {
                    throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "invalid argument: assembly {0} cannot be found", assemblyName));
                }
                taskDlls.Add(arg);
            }

            taskDlls.Add(GetAssemblyName(typeof(ITask)));
            taskDlls.Add(GetAssemblyName(typeof(HelloTask)));
            taskDlls.Add(GetAssemblyName(typeof(ShellTask)));
            taskDlls.Add(GetAssemblyName(typeof(StreamTask1)));

            IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(taskDlls.ToArray());

            // the following is verification only
            // to verify that a class indeeded has been added to the class hierarchy, check the class name
            IClassNode streamTaskClassNode = (IClassNode)ns.GetNode(typeof(StreamTask1).AssemblyQualifiedName);
            IClassNode helloTaskClassNode = (IClassNode)ns.GetNode(typeof(HelloTask).AssemblyQualifiedName);
            IClassNode shellTaskClassNode = (IClassNode)ns.GetNode(typeof(ShellTask).AssemblyQualifiedName);

            ProtocolBufferClassHierarchy.Serialize(ClassHierarchyBinFileName, ns);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize(ClassHierarchyBinFileName);

            IClassNode retrievedStreamTaskClassNode = (IClassNode)ch.GetNode(typeof(StreamTask1).AssemblyQualifiedName);
            IClassNode retrievedHelloTaskClassNode = (IClassNode)ch.GetNode(typeof(HelloTask).AssemblyQualifiedName);
            IClassNode retrievedShellTaskClassNode = (IClassNode)ch.GetNode(typeof(ShellTask).AssemblyQualifiedName);

            if (!streamTaskClassNode.GetFullName().Equals(retrievedStreamTaskClassNode.GetFullName()) ||
                !helloTaskClassNode.GetFullName().Equals(retrievedHelloTaskClassNode.GetFullName()) ||
                !shellTaskClassNode.GetFullName().Equals(retrievedShellTaskClassNode.GetFullName()))              
            {
                Console.WriteLine("Node deseriliazed is not equal");
            }
            else
            {
                Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Class hierarchy written to [{0}].", Directory.GetCurrentDirectory()));
            }
        }

        private static string GetAssemblyName(Type type)
        {
            return type.Assembly.GetName().Name;
        }
    }
}
