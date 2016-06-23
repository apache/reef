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
using System.Globalization;
using System.IO;
using System.Linq;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans
{
    public class PartialMean
    {
        public PartialMean(DataVector vector, int size)
        {
            Mean = vector;
            Size = size;
        }

        public PartialMean()
        {
        }

        public DataVector Mean { get; set; }

        public int Size { get; set; }

        public static PartialMean FromString(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                throw new ArgumentException("str");
            }
            string[] parts = str.Split('#');
            if (parts == null || parts.Length != 2)
            {
                throw new ArgumentException("Cannot deserialize PartialMean from string " + str);
            }
            return new PartialMean(DataVector.FromString(parts[0]), int.Parse(parts[1], CultureInfo.InvariantCulture));
        }

        public static PartialMean AggregatedPartialMean(List<PartialMean> means)
        {
            if (means == null || means.Count == 0)
            {
                throw new ArgumentException("means");
            }
            PartialMean mean = means[0];
            for (int i = 1; i < means.Count; i++)
            {
                mean = mean.CombinePartialMean(means[i]);
            }
            return mean;
        }

        public static DataVector AggregatedMean(List<PartialMean> means)
        {
            return AggregatedPartialMean(means).Mean;
        }

        public static List<DataVector> AggregateTrueMeansToFileSystem(int partitionsNum, int clustersNum, string executionDirectory)
        {
            List<PartialMean> partialMeans = new List<PartialMean>();
            for (int i = 0; i < partitionsNum; i++)
            {
                // should be replaced with Group Communication
                string path = Path.Combine(executionDirectory, Constants.DataDirectory, Constants.PartialMeanFilePrefix + i.ToString(CultureInfo.InvariantCulture));
                FileStream file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                using (StreamReader reader = new StreamReader(file))
                {
                    int index = 0;
                    while (!reader.EndOfStream)
                    {
                        string line = reader.ReadLine();
                        if (index++ < clustersNum)
                        {
                            partialMeans.Add(PartialMean.FromString(line));
                        }
                    }
                    reader.Close();
                }
            }
            List<DataVector> newCentroids = new List<DataVector>();
            for (int i = 0; i < clustersNum; i++)
            {
                List<PartialMean> means = partialMeans.Where(m => m.Mean.Label == i).ToList();
                newCentroids.Add(PartialMean.AggregatedMean(means));
            }
            return newCentroids;
        }

        public override string ToString()
        {
            return Mean.ToString() + "#" + Size;
        }

        private PartialMean CombinePartialMean(PartialMean other)
        {
            PartialMean aggreatedMean = new PartialMean();
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }
            if (Mean.Label != other.Mean.Label)
            {
                throw new ArgumentException("cannot combine partial means with different labels");
            }
            aggreatedMean.Size = Size + other.Size;
            aggreatedMean.Mean = Mean.MultiplyScalar(Size).Add(other.Mean.MultiplyScalar(other.Size)).Normalize(aggreatedMean.Size);
            return aggreatedMean;
        }
    }
}
