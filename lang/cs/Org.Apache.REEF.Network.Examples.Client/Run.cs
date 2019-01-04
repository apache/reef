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
using Org.Apache.REEF.Network.Examples.GroupCommunication;

namespace Org.Apache.REEF.Network.Examples.Client
{
    public class Run
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("start running client: " + DateTime.Now);
            bool runOnYarn = false;
            int numNodes = 5;
            int startPort = 8900;
            int portRange = 1000;
            string testToRun = "ElasticBroadcastWithMultipleFailures";
            testToRun = testToRun.ToLower();

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
                    testToRun = args[4].ToLower();
                }
            }

            if (testToRun.Equals("RunPipelineBroadcastAndReduce".ToLower()) || testToRun.Equals("all"))
            {
                int arraySize = GroupTestConstants.ArrayLength;
                int chunkSize = GroupTestConstants.ChunkSize;

                if (args.Length > 5)
                {
                    arraySize = int.Parse(args[5]);
                    chunkSize = int.Parse(args[6]);
                }

                new PipelineBroadcastAndReduceClient().RunPipelineBroadcastAndReduce(runOnYarn, numNodes, startPort,
                    portRange, arraySize, chunkSize);
                Console.WriteLine("RunPipelineBroadcastAndReduce completed!!!");
            }

            if (testToRun.Equals("RunBroadcastAndReduce".ToLower()) || testToRun.Equals("all"))
            {
                new BroadcastAndReduceClient().RunBroadcastAndReduce(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("RunBroadcastAndReduce completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcast".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastClient(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticRunBroadcast completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcastWithFailureInConstructor".ToLower()) || testToRun.Equals("all"))
            {
                // This stage should fail
                new ElasticBroadcastClientWithFailureInConstructor(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticBroadcastWithFailureInConstructor completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcastWithFailureBeforeWorkflow".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastClientWithFailureBeforeWorkflow(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticBroadcastWithFailureBeforeWorkflow completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcastWithFailEvaluatorBeforeWorkflow".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastClientWithFailEvaluatorBeforeWorkflow(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticBroadcastWithFailEvaluatorBeforeWorkflow completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcastWithFailureBeforeBroadcast".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastClientWithFailureBeforeBroadcast(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticBroadcastWithFailureBeforeBroadcast completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcastWithFailureAfterBroadcast".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastClientWithFailureAfterBroadcast(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticBroadcastWithFailureAfterBroadcast completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcastWithFailureAfterBroadcast".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastClientWithFailureAfterBroadcast(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticBroadcastWithFailureAfterBroadcast completed!!!");
            }

            if (testToRun.Equals("ElasticBroadcastWithMultipleFailures".ToLower()) || testToRun.Equals("all"))
            {
                new ElasticBroadcastClientWithMultipleFailures(runOnYarn, numNodes, startPort, portRange);
                Console.WriteLine("ElasticBroadcastWithMultipleFailures completed!!!");
            }
        }
    }
}
