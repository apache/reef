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

using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Client.Tests
{
    [TestClass]
    public class WindowsYarnJobCommandBuilderTests
    {
        private static readonly List<string> AnyClassPathItems = new List<string>
        {
            "%HADOOP_CONF_DIR%",
            "%HADOOP_HOME%/*",
            "%HADOOP_HOME%/lib/*",
            "%HADOOP_COMMON_HOME%/*",
            "%HADOOP_COMMON_HOME%/lib/*",
            "%HADOOP_YARN_HOME%/*",
            "%HADOOP_YARN_HOME%/lib/*",
            "%HADOOP_HDFS_HOME%/*",
            "%HADOOP_HDFS_HOME%/lib/*",
            "%HADOOP_MAPRED_HOME%/*",
            "%HADOOP_MAPRED_HOME%/lib/*",
            "%HADOOP_HOME%/etc/hadoop",
            "%HADOOP_HOME%/share/hadoop/common/*",
            "%HADOOP_HOME%/share/hadoop/common/lib/*",
            "%HADOOP_HOME%/share/hadoop/yarn/*",
            "%HADOOP_HOME%/share/hadoop/yarn/lib/*",
            "%HADOOP_HOME%/share/hadoop/hdfs/*",
            "%HADOOP_HOME%/share/hadoop/hdfs/lib/*",
            "%HADOOP_HOME%/share/hadoop/mapreduce/*",
            "%HADOOP_HOME%/share/hadoop/mapreduce/lib/*",
            "reef/local/*",
            "reef/global/*"
        };

        [TestMethod]
        public void YarnJobCommandBuilderIsDefaultImplementationOfIYarnJobCommandBuilder()
        {
            Assert.IsInstanceOfType(TangFactory.GetTang().NewInjector().GetInstance<IYarnJobCommandBuilder>(), typeof(WindowsYarnJobCommandBuilder));
        }

        [TestMethod]
        public void GetJobSubmissionCommandGeneratesCorrectCommand()
        {
            var testContext = new TestContext();
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
                                           "che.reef.bridge.client.YarnBootstrapREEFLauncher reef/local/job-submis" +
                                           "sion-params.json 1> <LOG_DIR>/driver.stdout 2> <LOG_DIR>/driver.stderr";

            var commandBuilder = testContext.GetCommandBuilder();
            var jobSubmissionCommand = commandBuilder.GetJobSubmissionCommand();
            Assert.AreEqual(expectedCommand, jobSubmissionCommand);
        }

        [TestMethod]
        public void GetJobSubmissionCommandLoggingEnabledGeneratesCorrectCommand()
        {
            var testContext = new TestContext();
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
                                           "he.reef.bridge.client.YarnBootstrapREEFLauncher reef/local/job-submiss" +
                                           "ion-params.json 1> <LOG_DIR>/driver.stdout 2> <LOG_DIR>/driver.stderr";
            var commandBuilder = testContext.GetCommandBuilder(true);
            var jobSubmissionCommand = commandBuilder.GetJobSubmissionCommand();
            Assert.AreEqual(expectedCommand, jobSubmissionCommand);
        }

        [TestMethod]
        public void GetJobSubmissionCommandSetsCorrectDriverMemoryAllocationSize()
        {
            var testContext = new TestContext();
            const int sizeMB = 256;
            const string expectedCommandFormat = @"reef\Org.Apache.REEF.Bridge.exe" +
                                                 " %JAVA_HOME%/bin/java -XX:PermSize=128m -XX:MaxPermSize=128m -Xmx{0}m " +
                                                 "-classpath %HADOOP_CONF_DIR%;%HADOOP_HOME%/*;%HADOOP_HOME%/lib/*;%HADO" +
                                                 "OP_COMMON_HOME%/*;%HADOOP_COMMON_HOME%/lib/*;%HADOOP_YARN_HOME%/*;%HAD" +
                                                 "OOP_YARN_HOME%/lib/*;%HADOOP_HDFS_HOME%/*;%HADOOP_HDFS_HOME%/lib/*;%HA" +
                                                 "DOOP_MAPRED_HOME%/*;%HADOOP_MAPRED_HOME%/lib/*;%HADOOP_HOME%/etc/hadoo" +
                                                 "p;%HADOOP_HOME%/share/hadoop/common/*;%HADOOP_HOME%/share/hadoop/commo" +
                                                 "n/lib/*;%HADOOP_HOME%/share/hadoop/yarn/*;%HADOOP_HOME%/share/hadoop/y" +
                                                 "arn/lib/*;%HADOOP_HOME%/share/hadoop/hdfs/*;%HADOOP_HOME%/share/hadoop" +
                                                 "/hdfs/lib/*;%HADOOP_HOME%/share/hadoop/mapreduce/*;%HADOOP_HOME%/share" +
                                                 "/hadoop/mapreduce/lib/*;reef/local/*;reef/global/* -Dproc_reef org.apa" +
                                                 "che.reef.bridge.client.YarnBootstrapREEFLauncher reef/local/job-submis" +
                                                 "sion-params.json 1> <LOG_DIR>/driver.stdout 2> <LOG_DIR>/driver.stderr";
            string expectedCommand = string.Format(expectedCommandFormat, sizeMB);
            var commandBuilder = testContext.GetCommandBuilder().SetMaxDriverAllocationPoolSizeMB(sizeMB);
            var jobSubmissionCommand = commandBuilder.GetJobSubmissionCommand();
            Assert.AreEqual(expectedCommand, jobSubmissionCommand);
        }

        [TestMethod]
        public void GetJobSubmissionCommandSetsCorrectMaxPermSize()
        {
            var testContext = new TestContext();
            const int sizeMB = 256;
            const string expectedCommandFormat = @"reef\Org.Apache.REEF.Bridge.exe" +
                                                 " %JAVA_HOME%/bin/java -XX:PermSize=128m -XX:MaxPermSize={0}m -Xmx512m " +
                                                 "-classpath %HADOOP_CONF_DIR%;%HADOOP_HOME%/*;%HADOOP_HOME%/lib/*;%HADO" +
                                                 "OP_COMMON_HOME%/*;%HADOOP_COMMON_HOME%/lib/*;%HADOOP_YARN_HOME%/*;%HAD" +
                                                 "OOP_YARN_HOME%/lib/*;%HADOOP_HDFS_HOME%/*;%HADOOP_HDFS_HOME%/lib/*;%HA" +
                                                 "DOOP_MAPRED_HOME%/*;%HADOOP_MAPRED_HOME%/lib/*;%HADOOP_HOME%/etc/hadoo" +
                                                 "p;%HADOOP_HOME%/share/hadoop/common/*;%HADOOP_HOME%/share/hadoop/commo" +
                                                 "n/lib/*;%HADOOP_HOME%/share/hadoop/yarn/*;%HADOOP_HOME%/share/hadoop/y" +
                                                 "arn/lib/*;%HADOOP_HOME%/share/hadoop/hdfs/*;%HADOOP_HOME%/share/hadoop" +
                                                 "/hdfs/lib/*;%HADOOP_HOME%/share/hadoop/mapreduce/*;%HADOOP_HOME%/share" +
                                                 "/hadoop/mapreduce/lib/*;reef/local/*;reef/global/* -Dproc_reef org.apa" +
                                                 "che.reef.bridge.client.YarnBootstrapREEFLauncher reef/local/job-submis" +
                                                 "sion-params.json 1> <LOG_DIR>/driver.stdout 2> <LOG_DIR>/driver.stderr";
            string expectedCommand = string.Format(expectedCommandFormat, sizeMB);
            var commandBuilder = testContext.GetCommandBuilder().SetDriverMaxPermSizeMB(sizeMB);
            var jobSubmissionCommand = commandBuilder.GetJobSubmissionCommand();
            Assert.AreEqual(expectedCommand, jobSubmissionCommand);
        }

        private class TestContext
        {
            public readonly IYarnCommandLineEnvironment YarnCommandLineEnvironment = Substitute.For<IYarnCommandLineEnvironment>();

            public WindowsYarnJobCommandBuilder GetCommandBuilder(bool enableLogging = false)
            {
                var injector = TangFactory.GetTang().NewInjector();
                YarnCommandLineEnvironment.GetYarnClasspathList().Returns(AnyClassPathItems);
                injector.BindVolatileInstance(GenericType<IYarnCommandLineEnvironment>.Class, YarnCommandLineEnvironment);
                injector.BindVolatileParameter(GenericType<EnableDebugLogging>.Class, enableLogging);
                return injector.GetInstance<WindowsYarnJobCommandBuilder>();
            }
        }
    }
}