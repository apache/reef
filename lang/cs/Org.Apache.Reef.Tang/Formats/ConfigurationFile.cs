/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
﻿using Org.Apache.Reef.Utilities.Logging;
﻿using Org.Apache.Reef.Tang.Exceptions;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Types;
﻿using Org.Apache.Reef.Tang.Util;

namespace Org.Apache.Reef.Tang.Formats
{
    public class ConfigurationFile
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ConfigurationFile));

        //#region Avro serialization
        //public static string AvroSerialize(IConfiguration c)
        //{
        //    var obj = new ConfigurationDataContract(ToConfigurationStringList(c));
        //    var serializer = AvroSerializer.Create<ConfigurationDataContract>();
        //    var schema = serializer.WriterSchema.ToString();

        //    var stream = new MemoryStream();
        //    serializer.Serialize(stream, obj);
        //    return Convert.ToBase64String(stream.GetBuffer());
        //}

        //public static void AvroDeseriaize(IConfigurationBuilder conf, string serializedConfig)
        //{
        //    var serializer2 = AvroSerializer.Create<ConfigurationDataContract>();
        //    ConfigurationDataContract confgDataObj;
        //    using (var stream2 = new MemoryStream(Convert.FromBase64String(serializedConfig)))
        //    {
        //        confgDataObj = serializer2.Deserialize(stream2);
        //    }

        //    IList<KeyValuePair<string, string>> settings = new List<KeyValuePair<string, string>>();

        //    foreach (string line in confgDataObj.Bindings)
        //    {
        //        string[] p = line.Split('=');
        //        settings.Add(new KeyValuePair<string, string>(p[0], p[1]));
        //    }
        //    ProcessConfigData(conf, settings);
        //}

        //public static IConfiguration AvroDeseriaize(string serializedConfig)
        //{
        //    ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
        //    AvroDeseriaize(cb, serializedConfig);
        //    return cb.Build();
        //}

        //public static IConfiguration AvroDeseriaizeFromFile(string configFileName)
        //{
        //    ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
        //    AddConfigurationFromFileUsingAvro(cb, configFileName);
        //    return cb.Build();
        //}

        //public static void WriteConfigurationFileUsingAvro(IConfiguration c, string fileName)
        //{
        //    using (FileStream aFile = new FileStream(fileName, FileMode.OpenOrCreate))
        //    {
        //        using (StreamWriter sw = new StreamWriter(aFile))
        //        {
        //            sw.Write(AvroSerialize(c));
        //        }
        //    }
        //}

        //public static void AddConfigurationFromFileUsingAvro(IConfigurationBuilder conf, string configFileName)
        //{
        //    string serializedString;
        //    using (StreamReader reader = new StreamReader(configFileName))
        //    {
        //        serializedString = reader.ReadLine();
        //    }
        //    AvroDeseriaize(conf, serializedString);
        //}
        //#endregion Avro serialization

        #region text file serialization
        public static void WriteConfigurationFile(IConfiguration c, string fileName)
        {
            using (FileStream aFile = new FileStream(fileName, FileMode.Create))
            {
                using (StreamWriter sw = new StreamWriter(aFile))
                {
                    sw.Write(ToConfigurationString(c));
                }
            }
        }

        public static String ToConfigurationString(IConfiguration c) 
        {
            StringBuilder sb = new StringBuilder();
            foreach (string s in ToConfigurationStringList(c)) 
            {
                sb.Append(s);
                sb.Append('\n');
            }
            return sb.ToString();
        }

        private static string GetFullName(INode n)
        {
            string s = n.GetFullName();
            Type t = ReflectionUtilities.GetTypeByName(s);
            return t.FullName;
        }
    

        private static string GetFullName(string name)
        {
            try
            {
                Type t = ReflectionUtilities.GetTypeByName(name);
                return t.FullName;
            }
            catch (ApplicationException e)
            {
                Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, LOGGER);
                return name;//if name is not a type, return as it was
                
            }
        }

        private static string GetAssemlyName(string s)
        {
            try
            {
                Type t = ReflectionUtilities.GetTypeByName(s);
                return ReflectionUtilities.GetAssemblyQualifiedName(t);
            }
            catch (ApplicationException e)
            {
                Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Caught(e, Level.Warning, LOGGER);
                return s;//if name is not a type, return as it was
            }
        }

        public static HashSet<String> ToConfigurationStringList(IConfiguration c) 
        {
            ConfigurationImpl conf = (ConfigurationImpl) c;
            HashSet<string> l = new HashSet<string>();
            foreach (IClassNode opt in conf.GetBoundImplementations()) 
            {
//                l.Add(opt.GetFullName() + '=' + Escape(conf.GetBoundImplementation(opt).GetFullName()));
                l.Add(GetFullName(opt) + '=' + Escape(GetFullName(conf.GetBoundImplementation(opt))));
            }
            
            foreach (IClassNode opt in conf.GetBoundConstructors()) 
            {
//                l.Add(opt.GetFullName() + '=' + Escape(conf.GetBoundConstructor(opt).GetFullName()));
                l.Add(GetFullName(opt) + '=' + Escape(GetFullName(conf.GetBoundConstructor(opt))));
            }
            foreach (INamedParameterNode opt in conf.GetNamedParameters()) 
            {
//                l.Add(opt.GetFullName() + '=' + Escape(conf.GetNamedParameter(opt)));
                l.Add(GetFullName(opt) + '=' + Escape(GetFullName(conf.GetNamedParameter(opt))));
            }
            foreach (IClassNode cn in conf.GetLegacyConstructors())
            {
                StringBuilder sb = new StringBuilder();
                Join(sb, "-", conf.GetLegacyConstructor(cn).GetArgs().ToArray<IConstructorArg>());
                l.Add(GetFullName(cn) + Escape('=' + ConfigurationBuilderImpl.INIT + '(' + sb.ToString() + ')'));
                //l.Add(cn.GetFullName() + Escape('=' + ConfigurationBuilderImpl.INIT + '(' + sb.ToString() + ')'));
                //s.append(cn.getFullName()).append('=').append(ConfigurationBuilderImpl.INIT).append('(');
                //      .append(")\n");
            }


            IEnumerator bs = conf.GetBoundSets();
            while (bs.MoveNext())
            {
                KeyValuePair<INamedParameterNode, object> e = (KeyValuePair<INamedParameterNode, object>)bs.Current;

            //}
            //foreach (KeyValuePair<INamedParameterNode, object> e in conf.GetBoundSets()) 
            //{
                string val = null;
                if (e.Value is string) 
                {
                    val = GetFullName((string)e.Value);
                } 
                else if (e.Value is INode) 
                {
//                    val = ((INode)e.Value).GetFullName();
                    val = GetFullName((INode)e.Value);
                } 
                else 
                {
                    Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(), LOGGER);
                }
                
//                l.Add(e.Key.GetFullName() + '=' + Escape(val));
                l.Add(GetFullName(e.Key) + '=' + Escape(val));
                //      s.append(e.getKey().getFullName()).append('=').append(val).append("\n");
            }

            return l;//s.toString();
        }

        public static IConfiguration GetConfiguration(string configString)
        {
            byte[] array = Encoding.Default.GetBytes(configString);
            return GetConfiguration(array);
        }

        public static IConfiguration GetConfiguration(byte[] configStream)
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            AddConfigurationFromStream(cb, configStream);
            return cb.Build();
        }

        public static void AddConfigurationFromStream(IConfigurationBuilder conf, byte[] configData)
        {
            using (StreamReader reader = new StreamReader(new MemoryStream(configData), Encoding.Default))
            {
                 AddConfiguration(conf, reader);
            }
        }

        public static void AddConfigurationFromFile(IConfigurationBuilder conf, string configFileName)
        {            
            using (StreamReader reader = new StreamReader(configFileName))
            {
                 AddConfiguration(conf, reader);
            }
        }

        public static void AddConfigurationFromString(IConfigurationBuilder conf, string configData)
        {
            byte[] array = Encoding.ASCII.GetBytes(configData);
            AddConfigurationFromStream(conf, array);
        }

        private static void AddConfiguration(IConfigurationBuilder conf, StreamReader reader)
        {
            //IDictionary<string, string> settings = new Dictionary<string, string>();
            IList<KeyValuePair<string, string>> settings = new List<KeyValuePair<string, string>>();

            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                string[] p = line.Split('=');
                if (p.Length == 2)
                {
                    settings.Add(new KeyValuePair<string, string>(GetAssemlyName(p[0]), GetAssemlyName(p[1])));
                } 
                else if (p.Length > 2)
                {
                    string v = line.Substring(p[0].Length + 1, line.Length - p[0].Length - 1);
                    settings.Add(new KeyValuePair<string, string>(GetAssemlyName(p[0]), GetAssemlyName(v)));
                }
                else
                {
                    var e = new ApplicationException("Config data is not in format of KeyValuePair: " + line);
                    Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }  
            }
            ProcessConfigData(conf, settings);
        }

        public static IDictionary<string, string> FromFile(string fileName)
        {
            IDictionary<string, string> property = new Dictionary<string, string>();
            using (StreamReader sr = new StreamReader(fileName))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    string[] p = line.Split('=');
                    property.Add(ConfigurationFile.GetAssemlyName(p[0]), ConfigurationFile.GetAssemlyName(p[1]));
                }
            }
            return property;
        }

        public static void ProcessConfigData(IConfigurationBuilder conf, IList<KeyValuePair<string, string>> settings)
        {
            foreach (KeyValuePair<string, string> kv in settings)
            {
                try
                {
                    conf.Bind(kv.Key, kv.Value);
                }
                catch (BindException ex)
                {
                    Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                    var e = new BindException("Failed to process configuration tuple: [" + kv.Key + "=" + kv.Value + "]", ex);
                    Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
                catch (ClassHierarchyException ex)
                {
                    Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                    var e = new ClassHierarchyException("Failed to process configuration tuple: [" + kv.Key + "=" + kv.Value + "]", ex);
                    Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }
        }

        public static void ProcessConfigData(IConfigurationBuilder conf, IDictionary<string, string> settings)
        {
            IList<KeyValuePair<string, string>> list = new List<KeyValuePair<string, string>>();

            foreach (KeyValuePair<string, string> kv in settings)
            {
                list.Add(kv);
            }

            ProcessConfigData(conf, list);
        }
 
       /**
        * Replace any \'s in the input string with \\. and any "'s with \".
        * @param in
        * @return
        */
        private static string Escape(string str) 
        {
            return str;  //TODO
            // After regexp escaping \\\\ = 1 slash, \\\\\\\\ = 2 slashes.

            // Also, the second args of replaceAll are neither strings nor regexps, and
            // are instead a special DSL used by Matcher. Therefore, we need to double
            // escape slashes (4 slashes) and quotes (3 slashes + ") in those strings.
            // Since we need to write \\ and \", we end up with 8 and 7 slashes,
            // respectively.
            //return in.ReplaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\\\"");
        }

        public static StringBuilder Join(StringBuilder sb, String sep, IConstructorArg[] types) 
        {
            if (types.Length > 0) 
            {
                sb.Append(types[0].GetType());
                for (int i = 1; i < types.Length; i++) 
                {
                    sb.Append(sep).Append(types[i].GetType());
                }
            }
            return sb;
        }
        #endregion text file serialization
    }
}