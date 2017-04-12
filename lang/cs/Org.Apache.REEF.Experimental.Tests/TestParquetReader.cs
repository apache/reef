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

using System.IO;
using System.Runtime.Serialization;
using Org.Apache.REEF.Experimental.ParquetReader;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.IO.TempFileCreation;
using Xunit;

namespace Org.Apache.REEF.Experimental.Tests
{
    public sealed class TestParquetReader
    {

        [DataContract(Name = "User", Namespace = "parquetreader")]
        internal class User
        {
            [DataMember(Name = "name")]
            public string name { get; set; }

            [DataMember(Name = "age")]
            public int age { get; set; }

            [DataMember(Name = "favorite_color")]
            public string favorite_color { get; set; }
        }

        [Fact]
        public void TestReadParquetFile()
        {
            // Create a tmp avro file
            var fb = TempFileConfigurationModule.ConfigurationModule.Build();
            var fi = TangFactory.GetTang().NewInjector(fb);
            var tempFileCreator = fi.GetInstance<ITempFileCreator>();
            var tmpFile = tempFileCreator.GetTempFileName();
            var avroPath = Path.GetFullPath(tmpFile);

            // Identify parquet file and jar path
            var dir = new DirectoryInfo(Directory.GetCurrentDirectory());
            while (!dir.FullName.EndsWith(@"reef\lang"))
            {
                dir = Directory.GetParent(dir.FullName);
            }
            var parquetPath = dir.FullName + @"\java\reef-experimental\src\test\resources\file.parquet";
            var jarPath = dir.FullName + @"\java\reef-experimental\target\reef-experimental-0.16.0-SNAPSHOT-jar-with-dependencies.jar";

            ITang tang = TangFactory.GetTang();
            ICsConfigurationBuilder cb = tang.NewConfigurationBuilder(new string[] { "ParquetReader Test" });
            cb.BindNamedParameter<ParquetPathString, string>(GenericType<ParquetPathString>.Class, parquetPath);
            cb.BindNamedParameter<AvroPathString, string>(GenericType<AvroPathString>.Class, avroPath);
            cb.BindNamedParameter<JarPathString, string>(GenericType<JarPathString>.Class, jarPath);
            IConfiguration conf = cb.Build();
            IInjector injector = tang.NewInjector(conf);

            var reader = (ParquetReader.ParquetReader)injector.GetInstance(typeof(ParquetReader.ParquetReader));
            var i = 0;
            foreach (var obj in reader.read<User>())
            {
                Assert.Equal(obj.name, "User_" + i);
                Assert.Equal(obj.age, i);
                Assert.Equal(obj.favorite_color, "blue");
                i++;
            }
            Assert.Equal(i, 10);
        }
    }
}
