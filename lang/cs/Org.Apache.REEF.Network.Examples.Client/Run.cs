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

using Org.Apache.REEF.Network.Examples.GroupCommunication;
using System;

namespace Org.Apache.REEF.Network.Examples.Client
{
    internal enum TestType
    {
        PipelineBroadcastAndReduce,
        BroadcastAndReduce,
        ElasticBroadcast,
        ElasticBroadcastWithFailureInConstructor,
        ElasticBroadcastWithFailureBeforeWorkflow,
        ElasticBroadcastWithFailEvaluatorBeforeWorkflow,
        ElasticBroadcastWithFailureBeforeBroadcast,
        ElasticBroadcastWithFailureAfterBroadcast,
        ElasticBroadcastWithMultipleFailures
    }

    public class Run
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("start running client: " + DateTime.Now);
            bool runOnYarn = false;
            int numNodes = 5;
            int startPort = 8900;
            int portRange = 1000;
            string testToRun = "ElasticBroadcastWithFailEvaluatorBeforeWorkflow";

            if (args != null)
            {
                if (args.Length > 0)
                {
                    runOnYarn = bool.Parse(args[0].ToLower());
                }

                if (args.Length > 1)
                {
                    numNodes = int.Parse(args[1]);
                }

                if (args.Length > 2)
                {
                    startPort = int.Parse(args[2]);
                }

                if (args.Length > 3)
                {
                    portRange = int.Parse(args[3]);
                }

                if (args.Length > 4)
                {
                    testToRun = args[4];
                }
            }

            if (TestType.PipelineBroadcastAndReduce.Match(testToRun))
            {
                int arraySize = GroupTestConstants.ArrayLength;
                int chunkSize = GroupTestConstants.ChunkSize;

                if (args.Length > 5)
                {
                    arraySize = int.Parse(args[5]);
                    chunkSize = int.Parse(args[6]);
                }

                new PipelineBroadcastAndReduceClient().RunPipelineBroadcastAndReduce(
                    runOnYarn, 
                    numNodes, 
                    startPort,
                    portRange, 
                    arraySize, 
                    chunkSize);
                Console.WriteLine("PipelineBroadcastAndReduce completed!!!");
            }

            if (TestType.BroadcastAndReduce.Match(testToRun))
            {
                new BroadcastAndReduceClient().RunBroadcastAndReduce(
                    runOnYarn, 
                    numNodes, 
                    startPort, 
                    portRange);
                Console.WriteLine("BroadcastAndReduce completed!!!");
            }

            if (TestType.ElasticBroadcast.Match(testToRun))
            {
                new ElasticBroadcastClient(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticBroadcast completed!!!");
            }

            if (TestType.ElasticBroadcastWithFailureInConstructor.Match(testToRun))
            {
                new ElasticBroadcastClientWithFailureInConstructor(
                    runOnYarn, 
                    numNodes, 
                    startPort, 
                    portRange);
                Console.WriteLine("ElasticBroadcastWithFailureInConstructor completed!!!");
            }

            if (TestType.ElasticBroadcastWithFailureBeforeWorkflow.Match(testToRun))
            {
                new ElasticBroadcastClientWithFailureBeforeWorkflow(
                    runOnYarn, 
                    numNodes, 
                    startPort, 
                    portRange);
                Console.WriteLine("ElasticBroadcastWithFailureBeforeWorkflow completed!!!");
            }

            if (TestType.ElasticBroadcastWithFailEvaluatorBeforeWorkflow.Match(testToRun))
            {
                new ElasticBroadcastClientWithFailEvaluatorBeforeWorkflow(
                    runOnYarn, 
                    numNodes, 
                    startPort, 
                    portRange);
                Console.WriteLine("ElasticBroadcastWithFailEvaluatorBeforeWorkflow completed!!!");
            }

            if (TestType.ElasticBroadcastWithFailureBeforeBroadcast.Match(testToRun))
            {
                new ElasticBroadcastClientWithFailureBeforeBroadcast(
                    runOnYarn, 
                    numNodes, 
                    startPort, 
                    portRange);
                Console.WriteLine("ElasticBroadcastWithFailureBeforeBroadcast completed!!!");
            }

            if (TestType.ElasticBroadcastWithFailureAfterBroadcast.Match(testToRun))
            {
                new ElasticBroadcastClientWithFailureAfterBroadcast(
                    runOnYarn, 
                    numNodes, 
                    startPort, 
                    portRange);
                Console.WriteLine("ElasticBroadcastWithFailureAfterBroadcast completed!!!");
            }

            if (TestType.ElasticBroadcastWithMultipleFailures.Match(testToRun))
            {
                new ElasticBroadcastClientWithMultipleFailures(
                    runOnYarn, 
                    numNodes, 
                    startPort, 
                    portRange);
                Console.WriteLine("ElasticBroadcastWithMultipleFailures completed!!!");
            }
        }
    }

    internal static class TestTypeMatcher
    {
        public static bool Match(this TestType test, string name)
        {
            name = name.ToLower();
            return name.Equals("all") || test.ToString().ToLower().Equals(name);
        }
    }
}