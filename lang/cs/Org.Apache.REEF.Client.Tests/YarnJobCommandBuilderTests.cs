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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Client.Tests
{
    [TestClass]
    public class YarnJobCommandBuilderTests
    {
        [TestMethod]
        public void YarnJobCommandBuilderIsDefaultImplementationOfIYarnJobCommandBuilder()
        {
            Assert.IsInstanceOfType(TangFactory.GetTang().NewInjector().GetInstance<IYarnJobCommandBuilder>(), typeof(YarnJobCommandBuilder));
        }

        [TestMethod]
        public void GetJobSubmissionCommandGeneratesCorrectCommand()
        {
            const string expectedCommand = @"reef\Org.Apache.REEF.Bridge.exe" +
                                           " %JAVA_HOME%/bin/java -XX:PermSize=128m -XX:MaxPermSize=128m -Xmx512m " +
                                           "-classpath %HADOOP_CONF_DIR%;%HADOOP_HOME%/*;%HADOOP_HOME%/lib/*;%HADO" +
                                           "OP_COMMON_HOME%/*;%HADOOP_COMMON_HOME%/lib/*;%HADOOP_YARN_HOME%/*;%HAD" +
                                           "OOP_YARN_HOME%/lib/*;%HADOOP_HDFS_HOME%/*;%HADOOP_HDFS_HOME%/lib/*;%HA" +
                                           "DOOP_MAPRED_HOME%/*;%HADOOP_MAPRED_HOME%/lib/*;%HADOOP_HOME%/etc/hadoo" +
                                           "p;%HADOOP_HOME%/share/hadoop/common/*;%HADOOP_HOME%/share/hadoop/commo" +
                                           "n/lib/*;%HADOOP_HOME%/share/hadoop/yarn/*;%HADOOP_HOME%/share/hadoop/y" +
                                           "arn/lib/*;%HADOOP_HOME%/share/hadoop/hdfs/*;%HADOOP_HOME%/share/hadoop" +
                                           "/hdfs/lib/*;%HADOOP_HOME%/share/hadoop/mapreduce/*;%HADOOP_HOME%/share" +
                                           "/hadoop/mapreduce/lib/*;reef/local/*;reef/global/* -Dproc_reef org.apa" +
                                           "che.reef.bridge.client.YarnBootstrapREEFLauncher reef/local/yarnparame" +
                                           "ters.json 1> <LOG_DIR>/driver.stdout 2> <LOG_DIR>/driver.stderr";

            var commandBuilder = TangFactory.GetTang().NewInjector().GetInstance<YarnJobCommandBuilder>();
            var jobSubmissionCommand = commandBuilder.GetJobSubmissionCommand();
            Assert.AreEqual(expectedCommand, jobSubmissionCommand);
        }

        [TestMethod]
        public void GetJobSubmissionCommandLoggingEnabledGeneratesCorrectCommand()
        {
            const string expectedCommand = @"reef\Org.Apache.REEF.Bridge.exe" +
                                           " %JAVA_HOME%/bin/java -XX:PermSize=128m -XX:MaxPermSize=128m -Xmx512m " +
                                           "-classpath %HADOOP_CONF_DIR%;%HADOOP_HOME%/*;%HADOOP_HOME%/lib/*;%HADO" +
                                           "OP_COMMON_HOME%/*;%HADOOP_COMMON_HOME%/lib/*;%HADOOP_YARN_HOME%/*;%HAD" +
                                           "OOP_YARN_HOME%/lib/*;%HADOOP_HDFS_HOME%/*;%HADOOP_HDFS_HOME%/lib/*;%HA" +
                                           "DOOP_MAPRED_HOME%/*;%HADOOP_MAPRED_HOME%/lib/*;%HADOOP_HOME%/etc/hadoo" +
                                           "p;%HADOOP_HOME%/share/hadoop/common/*;%HADOOP_HOME%/share/hadoop/commo" +
                                           "n/lib/*;%HADOOP_HOME%/share/hadoop/yarn/*;%HADOOP_HOME%/share/hadoop/y" +
                                           "arn/lib/*;%HADOOP_HOME%/share/hadoop/hdfs/*;%HADOOP_HOME%/share/hadoop" +
                                           "/hdfs/lib/*;%HADOOP_HOME%/share/hadoop/mapreduce/*;%HADOOP_HOME%/share" +
                                           "/hadoop/mapreduce/lib/*;reef/local/*;reef/global/* -Dproc_reef -Djava." +
                                           "util.logging.config.class=org.apache.reef.util.logging.Config org.apac" +
                                           "he.reef.bridge.client.YarnBootstrapREEFLauncher reef/local/yarnparamet" +
                                           "ers.json 1> <LOG_DIR>/driver.stdout 2> <LOG_DIR>/driver.stderr";
            var injector = TangFactory.GetTang().NewInjector();
            injector.BindVolatileParameter(GenericType<EnableDebugLogging>.Class, true);
            var commandBuilder = injector.GetInstance<YarnJobCommandBuilder>();
            var jobSubmissionCommand = commandBuilder.GetJobSubmissionCommand();
            Assert.AreEqual(expectedCommand, jobSubmissionCommand);
        }
    }
}