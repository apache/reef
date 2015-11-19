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

using System;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IO.TestClient
{
    // TODO[JIRA REEF-815]: once move to Nunit, tose test should be moved to Test project
    /// <summary>
    /// This purpose of this test is to run tests in Yarn envionment
    /// 
    /// </summary>
    public class Run
    {    
        private static void Main(string[] args)
        {
            if (args.Length > 0 && args[0].Equals("p"))
            {
                bool result = HadoopFileInputPartitionTest.TestWithByteDeserializer();
                Assert.IsTrue(result);
            }

            // the remote path name can be either full or relative, passing with the second argument
            if (args.Length > 1 && args[0].Equals("c"))
            {
                HadoopFileSystemTest t = new HadoopFileSystemTest();
                Assert.IsTrue(t.TestCopyFromRemote(args[1]));
            }

            if (args.Length > 0 && args[0].Equals("b"))
            {
                HadoopFileSystemTest t = new HadoopFileSystemTest();
                Assert.IsTrue(t.TestCopyFromLocalAndBack());
            }            
        }
    }

    public static class Assert
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(Assert));

        public static void IsTrue(bool condition, string message = null)
        {
            if (!condition)
            {
                Logger.Log(Level.Error, "Assert failed; {0}", message);
                throw new AssertFailedException(message);
            }
        }
    }

    public class AssertFailedException : Exception
    {
        public AssertFailedException(string message) : base(message) { }
    }
}
