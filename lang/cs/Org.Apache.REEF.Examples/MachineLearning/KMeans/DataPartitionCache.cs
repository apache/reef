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
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans
{   
    // TODO: we should outsource some of the functionalites to a data loader implemenation
    public class DataPartitionCache
    {
        private static readonly Logger _Logger = Logger.GetLogger(typeof(DataPartitionCache));

        [Inject]
        public DataPartitionCache(
            [Parameter(Value = typeof(PartitionIndex))] int partition,
            [Parameter(Value = typeof(KMeansConfiguratioinOptions.ExecutionDirectory))] string executionDirectory)
        {
            Partition = partition;
            if (Partition < 0)
            {
                _Logger.Log(Level.Info, "no data to load since partition = " + Partition);
            }
            else
            {
                _Logger.Log(Level.Info, "loading data for partition " + Partition);
                DataVectors = loadData(partition, executionDirectory);
            }
        }

        public List<DataVector> DataVectors { get; set; }

        public int Partition { get; set; }

        // read initial data from file and marked it as unlabeled (not associated with any centroid)
        public static List<DataVector> ReadDataFile(string path, char seperator = ',')
        {
            List<DataVector> data = new List<DataVector>();
            FileStream file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            using (StreamReader reader = new StreamReader(file))
            {
                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        data.Add(DataVector.FromString(line));
                    }
                }
                reader.Close();
            }
            
            return data;
        }

        public void LabelData(Centroids centroids)
        {
            foreach (DataVector vector in DataVectors)
            {
                float minimumDistance = float.MaxValue;
                foreach (DataVector centroid in centroids.Points)
                {
                    float d = vector.DistanceTo(centroid);
                    if (d < minimumDistance)
                    {
                        vector.Label = centroid.Label;
                        minimumDistance = d;
                    }
                }
            }
        }

        private List<DataVector> loadData(int partition, string executionDirectory)
        {
            string file = Path.Combine(executionDirectory, Constants.DataDirectory, partition.ToString(CultureInfo.InvariantCulture));
            return ReadDataFile(file);
        }

        [NamedParameter("Data partition index", "partition")]
        public class PartitionIndex : Name<int>
        {
        }
    }
}
