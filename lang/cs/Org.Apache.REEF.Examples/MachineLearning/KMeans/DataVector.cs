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
    public class DataVector
    {
        public DataVector(int dimension, int label = -1)
        {
            Dimension = dimension;
            Data = Enumerable.Repeat((float)0, Dimension).ToList();
            Label = label;
        }

        // unlabeled data
        public DataVector(List<float> data) : this(data, -1)
        {
        }

        public DataVector(List<float> data, int label)
        {
            if (data == null || data.Count == 0)
            {
                throw new ArgumentNullException("data");
            }
            Dimension = data.Count;
            Data = data;
            Label = label;
        }

        public List<float> Data { get; set; }

        public int Label { get; set; }

        public int Dimension { get; set; }

        public static float TotalDistance(List<DataVector> list1, List<DataVector> list2)
        {
            if (list1 == null || list2 == null || list1.Count == 0 || list2.Count == 0)
            {
                throw new ArgumentException("one of the input list is null or empty");
            }
            if (list1.Count != list2.Count)
            {
                throw new ArgumentException("list 1's dimensionality does not mach list 2.");
            }
            float distance = 0;
            for (int i = 0; i < list1.Count; i++)
            {
                distance += list1[i].DistanceTo(list2[i]);
            }
            return distance;
        }

        public static DataVector Mean(List<DataVector> vectors)
        {
            if (vectors == null || vectors.Count == 0)
            {
                throw new ArgumentNullException("vectors");
            }
            DataVector mean = new DataVector(vectors[0].Dimension);
            for (int i = 0; i < vectors.Count; i++)
            {
                mean = mean.Add(vectors[i], ignoreLabel: true);
            }
            return mean.Normalize(vectors.Count);
        }

        // shuffle data and write them to different partions (different files on disk for now)
        public static List<DataVector> ShuffleDataAndGetInitialCentriods(string originalDataFile, int partitionsNum, int clustersNum, string executionDirectory)
        {
            List<DataVector> data = DataPartitionCache.ReadDataFile(originalDataFile);

            // shuffle, not truly random, but sufficient for our purpose
            data = data.OrderBy(a => Guid.NewGuid()).ToList();
            string dataDirectory = Path.Combine(executionDirectory, Constants.DataDirectory);

            // clean things up first
            if (Directory.Exists(dataDirectory))
            {
                Directory.Delete(dataDirectory, true);
            }
            Directory.CreateDirectory(dataDirectory);

            int residualCount = data.Count;
            int batchSize = data.Count / partitionsNum;
            for (int i = 0; i < partitionsNum; i++)
            {
                int linesCount = residualCount > batchSize ? batchSize : residualCount;
                using (StreamWriter writer = new StreamWriter(
                    File.OpenWrite(Path.Combine(executionDirectory, Constants.DataDirectory, i.ToString(CultureInfo.InvariantCulture)))))
                {
                    for (int j = i * batchSize; j < (i * batchSize) + linesCount; j++)
                    {
                        writer.WriteLine(data[j].ToString());
                    }
                    writer.Close();
                }
            }
            return InitializeCentroids(clustersNum, data, executionDirectory);
        }

        public static void WriteToCentroidFile(List<DataVector> centroids, string executionDirectory)
        {
            string centroidFile = Path.Combine(executionDirectory, Constants.CentroidsFile);
            File.Delete(centroidFile);
            using (StreamWriter writer = new StreamWriter(File.OpenWrite(centroidFile)))
            {
                foreach (DataVector dataVector in centroids)
                {
                    writer.WriteLine(dataVector.ToString());
                }
                writer.Close();
            }
        }

        // TODO: replace with proper deserialization
        public static DataVector FromString(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                throw new ArgumentException("str");
            }
            string[] dataAndLable = str.Split(';');
            if (dataAndLable == null || dataAndLable.Length > 2)
            {
                throw new ArgumentException("Cannot deserialize DataVector from string " + str);
            }
            int label = -1;
            if (dataAndLable.Length == 2)
            {
                label = int.Parse(dataAndLable[1], CultureInfo.InvariantCulture);
            }
            List<float> data = dataAndLable[0].Split(',').Select(float.Parse).ToList();
            return new DataVector(data, label);
        }

        // by default use squared euclidean disatance 
        // a naive implemenation without considering things like data normalization or overflow 
        // and it is not particular about efficiency
        public float DistanceTo(DataVector other)
        {
            VectorsArithmeticPrecondition(other);
            float d = 0;
            for (int i = 0; i < Data.Count; i++)
            {
                float diff = Data[i] - other.Data[i];
                d += diff * diff;
            }
            return d;
        }

        public float DistanceTo(List<DataVector> list)
        {
            float distance = 0;
            for (int i = 0; i < list.Count; i++)
            {
                distance += this.DistanceTo(list[i]);
            }
            return distance;
        }

        public DataVector Add(DataVector other, bool ignoreLabel = false)
        {
            VectorsArithmeticPrecondition(other);
            if (!ignoreLabel)
            {
                if (Label != other.Label)
                {
                    throw new InvalidOperationException("by default cannot apply addition operation on data of different labels.");
                }
            }
            List<float> sumData = new List<float>(Data);
            for (int i = 0; i < Data.Count; i++)
            {
                sumData[i] += other.Data[i];
            }
            return new DataVector(sumData, ignoreLabel ? -1 : Label);
        }

        public DataVector Normalize(float normalizationFactor)
        {
            if (normalizationFactor == 0)
            {
                throw new InvalidOperationException("normalizationFactor is zero");
            }
            DataVector result = new DataVector(Data, Label);
            for (int i = 0; i < Data.Count; i++)
            {
                result.Data[i] /= normalizationFactor;
            }
            return result;
        }

        public DataVector MultiplyScalar(float scalar)
        {
            DataVector result = new DataVector(Data, Label);
            for (int i = 0; i < Data.Count; i++)
            {
                result.Data[i] *= scalar;
            }
            return result;
        }

        // TODO: replace with proper serialization
        public override string ToString()
        {
            return string.Join(",", Data.Select(i => i.ToString(CultureInfo.InvariantCulture)).ToArray()) + ";" + Label;
        }

        // normally centroids are picked as random points from the vector space
        // here we just pick K random data samples
        private static List<DataVector> InitializeCentroids(int clustersNum, List<DataVector> data, string executionDirectory)
        {
            // again we used the not-some-random guid trick, 
            // not truly random and not quite efficient, but easy to implement as v1
            List<DataVector> centroids = data.OrderBy(a => Guid.NewGuid()).Take(clustersNum).ToList();

            // add label to centroids
            for (int i = 0; i < centroids.Count; i++)
            {
                centroids[i].Label = i;
            }
            WriteToCentroidFile(centroids, executionDirectory);
            return centroids;
        }

        private void VectorsArithmeticPrecondition(DataVector other)
        {
            if (other == null || other.Data == null)
            {
                throw new ArgumentNullException("other");
            }
            if (Data.Count != other.Data.Count)
            {
                throw new InvalidOperationException("vector dimentionality mismatch");
            }
        }
    }
}
