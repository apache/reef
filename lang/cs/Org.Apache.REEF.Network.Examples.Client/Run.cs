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
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Examples.Client
{
    public class Run
    {
        static void Main(string[] args)
        {
            Console.WriteLine("start running client: " + DateTime.Now);
            bool runOnYarn = false;
            List<string> testToRun = new List<string>();
            if (args != null)
            {
                if (args.Length > 0)
                {
                    runOnYarn = bool.Parse(args[0].ToLower());
                }

                for (int i = 1; i < args.Length; i++)
                {
                    testToRun.Add(args[i].ToLower());
                }
            }

            if (testToRun.Contains("RunPipelineBroadcastAndReduce".ToLower()) || testToRun.Contains("all") || testToRun.Count == 0)
            {
                new PipelineBroadcastAndReduceClient().RunPipelineBroadcastAndReduce(runOnYarn, 9);
                Console.WriteLine("RunPipelineBroadcastAndReduce completed!!!");
            }

            if (testToRun.Contains("RunBroadcastAndReduce".ToLower()) || testToRun.Contains("all") || testToRun.Count == 0)
            {
                new BroadcastAndReduceClient().RunBroadcastAndReduce(runOnYarn, 9);
                Console.WriteLine("RunBroadcastAndReduce completed!!!");
            }           
        }
    }
}
