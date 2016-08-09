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
using System.Collections;
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
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Formats
{
    public class AvroConfigurationResolver : Microsoft.Hadoop.Avro.AvroPublicMemberContractResolver
    {
        public override TypeSerializationInfo ResolveType(Type type)
        {
            var serInfo = base.ResolveType(type);
            serInfo.Aliases.Add("org.apache.reef.tang.formats.avro.AvroConfiguration");
            serInfo.Aliases.Add("org.apache.reef.tang.formats.avro.Bindings");
            serInfo.Aliases.Add("org.apache.reef.tang.formats.avro.ConfigurationEntry");

            return serInfo;
        }
    }

    public class AvroConfigurationSerializer : IConfigurationSerializer
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AvroConfigurationResolver));

        [Inject]
        public AvroConfigurationSerializer()
        {
        }

        public byte[] ToByteArray(IConfiguration c)
        {
            AvroConfiguration obj = ToAvroConfiguration(c);
            return AvroSerialize(obj);
        }

        public string GetSchema()
        {
            var serializer = AvroSerializer.Create<AvroConfiguration>();
            return serializer.WriterSchema.ToString();
        }

        public void ToFileStream(IConfiguration c, string fileName)
        {
            using (FileStream fs = new FileStream(fileName, FileMode.OpenOrCreate))
            {
                byte[] data = ToByteArray(c);
                fs.Write(data, 0, data.Length);
            }
        }

        public void ToFile(IConfiguration c, string fileName)
        {
            var avronConfigurationData = ToAvroConfiguration(c);
            using (var buffer = new MemoryStream())
            {
                using (var w = AvroContainer.CreateWriter<AvroConfiguration>(buffer, Codec.Null))
                {
                    using (var writer = new SequentialWriter<AvroConfiguration>(w, 24))
                    {
                        // Serialize the data to stream using the sequential writer
                        writer.Write(avronConfigurationData);
                    }
                }

                if (!WriteFile(buffer, fileName))
                {
                    var e = new TangApplicationException("Error during file operation. Quitting method: " + fileName);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }          
        }

        public IConfiguration FromByteArray(byte[] bytes)
        {
            AvroConfiguration avroConf = AvroDeserialize(bytes);
            return FromAvro(avroConf);
        }

        public IConfiguration AddFromByteArray(ICsConfigurationBuilder cb, byte[] bytes)
        {
            AvroConfiguration avroConf = AvroDeserialize(bytes);
            return AddFromAvro(cb, avroConf);
        }

        public IConfiguration FromFileStream(string fileName)
        {
            byte[] bytes = File.ReadAllBytes(fileName);
            AvroConfiguration avroConf = AvroDeserialize(bytes);
            return FromAvro(avroConf);
        }

        public IConfiguration FromFile(string fileName)
        {
            AvroConfiguration avroConf = AvroDeserializeFromFile(fileName);
            return FromAvro(avroConf);
        }

        public IConfiguration FromFile(string fileName, IClassHierarchy classHierarchy)
        {
            AvroConfiguration avroConf = AvroDeserializeFromFile(fileName);
            return FromAvro(avroConf, classHierarchy);
        }

        public string ToBase64String(IConfiguration c)
        {
            return Convert.ToBase64String(ToByteArray(c));
        }

        public IConfiguration FromBase64String(string serializedConfig)
        {
            var b = Convert.FromBase64String(serializedConfig);
            return FromByteArray(b);
        }

        public string ToString(IConfiguration c)
        {
            byte[] bytes = ToByteArray(c);
            AvroConfiguration avroConf = AvroDeserialize(bytes);
            string s = JsonConvert.SerializeObject(avroConf, Formatting.Indented);
            return s;
        }

        public IConfiguration FromString(string jsonString)
        {
            AvroConfiguration avroConf = JsonConvert.DeserializeObject<AvroConfiguration>(jsonString);
            return FromAvro(avroConf);
        }

        public IConfiguration FromString(string josonString, IClassHierarchy ch)
        {
            AvroConfiguration avroConf = JsonConvert.DeserializeObject<AvroConfiguration>(josonString);
            return FromAvro(avroConf, ch);
        }

        public AvroConfiguration AvroDeserializeFromFile(string fileName)
        {
            AvroConfiguration avroConf = null;
            try
            {
                using (var buffer = new MemoryStream())
                {
                    if (!ReadFile(buffer, fileName))
                    {
                        var e = new TangApplicationException("Error during file operation. Quitting method : " + fileName);
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }

                    buffer.Seek(0, SeekOrigin.Begin);
                    using (var reader = new SequentialReader<AvroConfiguration>(AvroContainer.CreateReader<AvroConfiguration>(buffer, true))) 
                    {
                        var results = reader.Objects;

                        if (results != null)
                        {
                            avroConf = (AvroConfiguration)results.First();
                        }
                    }
                }
            }
            catch (SerializationException ex)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(ex, Level.Error, LOGGER);
                var e = new TangApplicationException("Cannot deserialize the file: " + fileName, ex);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }

            return avroConf;
        }

        public IConfiguration FromAvro(AvroConfiguration avroConfiguration)
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();

            return AddFromAvro(cb, avroConfiguration);
        }

        public IConfiguration FromAvro(AvroConfiguration avroConfiguration, IClassHierarchy classHierarchy)
        {
            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(classHierarchy);

            return AddFromAvro(cb, avroConfiguration);
        }

        public AvroConfiguration ToAvroConfiguration(IConfiguration c)
        {
            ConfigurationImpl conf = (ConfigurationImpl)c;
            HashSet<ConfigurationEntry> l = new HashSet<ConfigurationEntry>();

            foreach (IClassNode opt in conf.GetBoundImplementations())
            {
                l.Add(new ConfigurationEntry(opt.GetFullName(), conf.GetBoundImplementation(opt).GetFullName()));
            }

            foreach (IClassNode opt in conf.GetBoundConstructors())
            {
                l.Add(new ConfigurationEntry(opt.GetFullName(), conf.GetBoundConstructor(opt).GetFullName()));
            }
            foreach (INamedParameterNode opt in conf.GetNamedParameters())
            {
                l.Add(new ConfigurationEntry(opt.GetFullName(), conf.GetNamedParameter(opt)));
            }
            foreach (IClassNode cn in conf.GetLegacyConstructors())
            {
                StringBuilder sb = new StringBuilder();
                ConfigurationFile.Join(sb, "-", conf.GetLegacyConstructor(cn).GetArgs().ToArray<IConstructorArg>());
                l.Add(new ConfigurationEntry(cn.GetFullName(), ConfigurationBuilderImpl.INIT + '(' + sb.ToString() + ')'));
            }

            IEnumerator bs = conf.GetBoundSets();
            while (bs.MoveNext())
            {
                KeyValuePair<INamedParameterNode, object> e = (KeyValuePair<INamedParameterNode, object>)bs.Current;

                string val = null;
                if (e.Value is string)
                {
                    val = (string)e.Value;
                }
                else if (e.Value is INode)
                {
                    val = ((INode)e.Value).GetFullName();
                }
                else
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(), LOGGER);
                }

                l.Add(new ConfigurationEntry(e.Key.GetFullName(), val));
            }

            return new AvroConfiguration(Language.Cs.ToString(), l);
        }
        
        private byte[] AvroSerialize(AvroConfiguration obj)
        {
            var serializer = AvroSerializer.Create<AvroConfiguration>();
            using (MemoryStream stream = new MemoryStream())
            {
                serializer.Serialize(stream, obj);
                return stream.ToArray();
            }
        }

        private AvroConfiguration AvroDeserialize(string serializedConfig)
        {
            return AvroDeserialize(Convert.FromBase64String(serializedConfig));
        }

        private AvroConfiguration AvroDeserialize(byte[] serializedBytes)
        {
            var serializer = AvroSerializer.Create<AvroConfiguration>();
            using (var stream = new MemoryStream(serializedBytes))
            {
                return serializer.Deserialize(stream);
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
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                return false;
            }
        }

        private IConfiguration AddFromAvro(IConfigurationBuilder cb, AvroConfiguration avroConfiguration)
        {
            IList<KeyValuePair<string, string>> settings = new List<KeyValuePair<string, string>>();

            foreach (ConfigurationEntry e in avroConfiguration.Bindings)
            {
                settings.Add(new KeyValuePair<string, string>(e.key, e.value));
            }
            ConfigurationFile.ProcessConfigData(cb, settings, avroConfiguration.language); 
            return cb.Build();
        }
    }
}