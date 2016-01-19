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
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using Newtonsoft.Json;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy.AvroDataContract;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    /// <summary>
    /// AvroClassHierarchySerializer is to serialize and deserialize ClassHierarchy
    /// </summary>
    public class AvroClassHierarchySerializer : IClassHierarchySerializer
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AvroClassHierarchySerializer));

        /// <summary>
        /// Default constructor for the interface IClassHierarchySerializer
        /// </summary>
        [Inject]
        private AvroClassHierarchySerializer()
        {
        }

        /// <summary>
        /// Avro schema
        /// </summary>
        /// <returns></returns>
        private string GetSchema()
        {
            var serializer = AvroSerializer.Create<AvroNode>();
            return serializer.WriterSchema.ToString();
        }

        /// <summary>
        /// Serialize a ClassHierarchy into a file 
        /// </summary>
        /// <param name="c"></param>
        /// <param name="fileName"></param>
        public void ToFile(IClassHierarchy c, string fileName)
        {
            var avroNodeData = ToAvroNode(c);
            using (var buffer = new MemoryStream())
            {
                using (var w = AvroContainer.CreateWriter<AvroNode>(buffer, Codec.Null))
                {
                    using (var writer = new SequentialWriter<AvroNode>(w, 24))
                    {
                        writer.Write(avroNodeData);
                    }
                }

                if (!WriteFile(buffer, fileName))
                {
                    var e = new ApplicationException("Error during file operation. Quitting method: " + fileName);
                    Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }
        }

        /// <summary>
        /// Serialize a ClassHierarchy into a text file as Json string
        /// </summary>
        /// <param name="c"></param>
        /// <param name="fileName"></param>
        public void ToTextFile(IClassHierarchy c, string fileName)
        {
            var fp = new StreamWriter(fileName);
            fp.WriteLine(ToString(c));
            fp.Close();
        }

        /// <summary>
        /// Serialize a ClassHierarchy into a byte array
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        public byte[] ToByteArray(IClassHierarchy c)
        {
            AvroNode obj = ToAvroNode(c);
            return AvroSerialize(obj);
        }

        /// <summary>
        /// Serialize a ClassHierarchy into a Json string
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        public string ToString(IClassHierarchy c)
        {
            AvroNode obj = ToAvroNode(c);
            string s = JsonConvert.SerializeObject(obj, Formatting.Indented);
            return s;
        }

        /// <summary>
        /// Deserialize a ClassHierarchy from a file
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public IClassHierarchy FromFile(string fileName)
        {
            AvroNode avroNode = AvroDeserializeFromFile(fileName);
            return FromAvroNode(avroNode);
        }

        /// <summary>
        /// Get Json string from the text file, the deserialize it into ClassHierarchy
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public IClassHierarchy FromTextFile(string fileName)
        {
            string line;
            StringBuilder b = new StringBuilder();

            StreamReader file = new StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                b.Append(line);
            }
            file.Close();

            return FromString(b.ToString());
        }

        /// <summary>
        /// Deserialize a ClassHierarchy from a byte array
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public IClassHierarchy FromByteArray(byte[] bytes)
        {
            AvroNode avroNode = AvroDeserialize(bytes);
            return FromAvroNode(avroNode);
        }

        /// <summary>
        /// Deserialize a ClassHierarchy from a Json string
        /// </summary>
        /// <param name="jsonString"></param>
        /// <returns></returns>
        public IClassHierarchy FromString(string jsonString)
        {
            AvroNode avroConf = JsonConvert.DeserializeObject<AvroNode>(jsonString);
            return FromAvroNode(avroConf);
        }

        /// <summary>
        /// Serialize a ClassHierarchy into AvroNode object
        /// </summary>
        /// <param name="ch"></param>
        /// <returns></returns>
        public AvroNode ToAvroNode(IClassHierarchy ch)
        {
            return NewAvroNode(ch.GetNamespace());
        }

        /// <summary>
        /// Deserialize ClassHierarchy from an AvroNode into AvroClassHierarchy object
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        public IClassHierarchy FromAvroNode(AvroNode n)
        {
            return new AvroClassHierarchy(n);
        }

        private AvroNode NewAvroNode(INode n)
        {
            IList<AvroNode> children = new List<AvroNode>();

            foreach (INode child in n.GetChildren())
            {
                children.Add(NewAvroNode(child));
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

                IList<AvroConstructorDef> injectableConstructors = new List<AvroConstructorDef>();
                foreach (IConstructorDef inj in injectable)
                {
                    injectableConstructors.Add(NewConstructorDef(inj));
                }

                IList<AvroConstructorDef> otherConstructors = new List<AvroConstructorDef>();
                foreach (IConstructorDef other in others)
                {
                    otherConstructors.Add(NewConstructorDef(other));
                }

                List<string> implFullNames = new List<string>();
                foreach (IClassNode impl in cn.GetKnownImplementations())
                {
                    implFullNames.Add(impl.GetFullName()); // we use class fully qualified name 
                }

                return NewClassNode(cn.GetName(), cn.GetFullName(),
                    cn.IsInjectionCandidate(), cn.IsExternalConstructor(), cn.IsUnit(),
                    injectableConstructors, otherConstructors, implFullNames, children);
            }

            if (n is INamedParameterNode)
            {
                INamedParameterNode np = (INamedParameterNode)n;
                return NewNamedParameterNode(np.GetName(), np.GetFullName(),
                    np.GetSimpleArgName(), np.GetFullArgName(), np.IsSet(), np.IsList(), np.GetDocumentation(),
                    np.GetShortName(), np.GetDefaultInstanceAsStrings(), children);
            }

            if (n is IPackageNode)
            {
                return NewPackageNode(n.GetName(), n.GetFullName(), children);
            }

            Utilities.Diagnostics.Exceptions.Throw(
                new IllegalStateException("Encountered unknown type of Node: " + n), LOGGER);
            return null;
        }

        private AvroNode NewClassNode(string name,
            string fullName,
            bool isInjectionCandidate,
            bool isExternalConstructor, bool isUnit,
            IList<AvroConstructorDef> injectableConstructors,
            IList<AvroConstructorDef> otherConstructors,
            IList<string> implFullNames, IList<AvroNode> children)
        {
            AvroClassNode classNode = new AvroClassNode();

            classNode.isInjectionCandidate = isInjectionCandidate;
            classNode.injectableConstructors = new List<AvroConstructorDef>();
            foreach (var ic in injectableConstructors)
            {
                classNode.injectableConstructors.Add(ic);
            }

            classNode.otherConstructors = new List<AvroConstructorDef>();
            foreach (var oc in otherConstructors)
            {
                classNode.otherConstructors.Add(oc);
            }

            classNode.implFullNames = new List<string>();
            foreach (var implFullName in implFullNames)
            {
                classNode.implFullNames.Add(implFullName);
            }

            AvroNode n = new AvroNode();
            n.name = name;
            n.fullName = fullName;
            n.classNode = classNode;

            n.children = new List<AvroNode>();
            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }

        private AvroNode NewNamedParameterNode(string name,
            string fullName, string simpleArgClassName, string fullArgClassName,
            bool isSet, bool isList, string documentation, // can be null
            string shortName, // can be null
            string[] instanceDefault, // can be null
            IList<AvroNode> children)
        {
            AvroNamedParameterNode namedParameterNode = new AvroNamedParameterNode();
            namedParameterNode.simpleArgClassName = simpleArgClassName;
            namedParameterNode.fullArgClassName = fullArgClassName;
            namedParameterNode.isSet = isSet;
            namedParameterNode.isList = isList;

            if (documentation != null)
            {
                namedParameterNode.documentation = documentation;
            }

            if (shortName != null)
            {
                namedParameterNode.shortName = shortName;
            }

            namedParameterNode.instanceDefault = new List<string>();
            foreach (var id in instanceDefault)
            {
                namedParameterNode.instanceDefault.Add(id);
            }

            AvroNode n = new AvroNode();
            n.name = name;
            n.fullName = fullName;
            n.namedParameterNode = namedParameterNode;

            n.children = new List<AvroNode>();
            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }

        private AvroNode NewPackageNode(string name,
            string fullName, IList<AvroNode> children)
        {
            AvroPackageNode packageNode = new AvroPackageNode();
            AvroNode n = new AvroNode();
            n.name = name;
            n.fullName = fullName;
            n.packageNode = packageNode;

            n.children = new List<AvroNode>();
            foreach (var c in children)
            {
                n.children.Add(c);
            }

            return n;
        }

        private AvroConstructorDef NewConstructorDef(IConstructorDef def)
        {
            IList<AvroConstructorArg> args = new List<AvroConstructorArg>();
            foreach (IConstructorArg arg in def.GetArgs())
            {
                args.Add(NewConstructorArg(arg.Gettype(), arg.GetNamedParameterName(), arg.IsInjectionFuture()));
            }

            AvroConstructorDef constDef = new AvroConstructorDef();
            constDef.fullClassName = def.GetClassName();

            constDef.constructorArgs = new List<AvroConstructorArg>();
            foreach (AvroConstructorArg arg in args)
            {
                constDef.constructorArgs.Add(arg);
            }

            return constDef;
        }

        private AvroConstructorArg NewConstructorArg(string fullArgClassName, string namedParameterName,
            bool isFuture)
        {
            AvroConstructorArg constArg = new AvroConstructorArg();
            constArg.fullArgClassName = fullArgClassName;
            constArg.namedParameterName = namedParameterName;
            constArg.isInjectionFuture = isFuture;
            return constArg;
        }

        private AvroNode AvroDeserializeFromFile(string fileName)
        {
            AvroNode avroNode = null;
            try
            {
                using (var buffer = new MemoryStream())
                {
                    if (!ReadFile(buffer, fileName))
                    {
                        var e =
                            new ApplicationException("Error during file operation. Quitting method : " + fileName);
                        Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }

                    buffer.Seek(0, SeekOrigin.Begin);
                    using (
                        var reader =
                            new SequentialReader<AvroNode>(AvroContainer.CreateReader<AvroNode>(buffer, true)))
                    {
                        var results = reader.Objects;

                        if (results != null)
                        {
                            avroNode = (AvroNode)results.First();
                        }
                    }
                }
            }
            catch (SerializationException ex)
            {
                Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                var e = new ApplicationException("Cannot deserialize the file: " + fileName, ex);
                Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }

            return avroNode;
        }

        private bool WriteFile(MemoryStream inputStream, string path)
        {
            try
            {
                using (FileStream fs = File.Create(path))
                {
                    inputStream.WriteTo(fs);
                }
                return true;
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                return false;
            }
        }

        private bool ReadFile(MemoryStream outputStream, string path)
        {
            try
            {
                byte[] data = File.ReadAllBytes(path);
                outputStream.Write(data, 0, data.Length);
                return true;
            }
            catch (Exception e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                return false;
            }
        }

        private byte[] AvroSerialize(AvroNode obj)
        {
            var serializer = AvroSerializer.Create<AvroNode>();
            using (MemoryStream stream = new MemoryStream())
            {
                serializer.Serialize(stream, obj);
                return stream.GetBuffer();
            }
        }

        private AvroNode AvroDeserialize(byte[] serializedBytes)
        {
            var serializer = AvroSerializer.Create<AvroNode>();

            using (var stream = new MemoryStream(serializedBytes))
            {
                return serializer.Deserialize(stream);
            }
        }
    }
}