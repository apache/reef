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
using System.ComponentModel;
using System.IO;
using Org.Apache.REEF.Network.Group.Codec;
using System.Runtime.Serialization;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Network.StreamingCodec;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators. This is the Writable version of GroupCommunicationMessage
    ///  class and will eventually replace it once everybody agrees with the design
    /// </summary>
    public class SerializableGroupCommunicationMessage : IWritable
    {
        /// <summary>
        /// Empty constructor to allow instantiation by reflection
        /// </summary>
        public SerializableGroupCommunicationMessage()
        {   
        }

        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="dataType">The type of message</param>
        /// <param name="message">The actual Writable message</param>
        /// <param name="messageType">The type of message to send</param>
        public SerializableGroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            Type dataType,
            IWritable message,
            MessageType messageType)
        {
            GroupName = groupName;
            OperatorName = operatorName;
            Source = source;
            Destination = destination;
            Data = new[] {message};
            MsgType = messageType;
            DataType = dataType.FullName;
            DataCount = 1;
        }

        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="dataType">The type of message</param>
        /// <param name="message">The actual Writable message array</param>
        /// <param name="messageType">The type of message to send</param>
        public SerializableGroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            Type dataType,
            IWritable[] message,
            MessageType messageType)
        {
            GroupName = groupName;
            OperatorName = operatorName;
            Source = source;
            Destination = destination;
            Data = message;
            MsgType = messageType;
            DataType = dataType.FullName;
            DataCount = Data.Length;
        }

        /// <summary>
        /// Returns the Communication Group name.
        /// </summary>
        public string GroupName { get; private set; }

        /// <summary>
        /// Returns the MPI Operator name.
        /// </summary>
        public string OperatorName { get; private set; }

        /// <summary>
        /// Returns the source of the message.
        /// </summary>
        public string Source { get; private set; }

        /// <summary>
        /// Returns the destination of the message.
        /// </summary>
        public string Destination { get; private set; }

        /// <summary>
        /// Returns the message DataType.
        /// </summary>
        public string DataType { get; private set; }

        /// <summary>
        /// Returns the number of messages.
        /// </summary>
        public int DataCount { get; private set; }

        /// <summary>
        /// Returns the array of messages.
        /// </summary>
        public IWritable[] Data { get; private set; }

        /// <summary>
        /// Returns the type of message being sent.
        /// </summary>
        public MessageType MsgType { get; private set; }

        /// <summary>
        /// Read the class fields from the stream.
        /// </summary>
        /// <param name="stream">The stream from which to read</param>
        /// <param name="optionalParameters">The optional parameters to be passed to the reader.
        /// There is no optional parameter for ths class</param>
        public void Read(Stream stream, params object[] optionalParameters)
        {
            AuxillaryStreamingFunctions.StringToStream(GroupName, stream);
            AuxillaryStreamingFunctions.StringToStream(OperatorName, stream);
            AuxillaryStreamingFunctions.StringToStream(Source, stream);
            AuxillaryStreamingFunctions.StringToStream(Destination, stream);
            AuxillaryStreamingFunctions.StringToStream(DataType, stream);
            AuxillaryStreamingFunctions.IntToStream(DataCount, stream);
            AuxillaryStreamingFunctions.StringToStream(MsgType.ToString(), stream);

            if (DataCount == 0)
            {
                throw new Exception("Data Count in Group COmmunication Message cannot be zero");
            }

            Data = new IWritable[DataCount];

            Type type = Type.GetType(DataType);

            if (type == null)
            {
                throw new Exception("Data type sepcified in Group Communication Message is null");
            }

            for (int index = 0; index < DataCount; index++)
            {
                Data[index] = Activator.CreateInstance(type) as IWritable;

                if (Data[index] == null)
                {
                    throw new Exception("IWritable instance cannot be created form the stream data in Group Communication Message");
                }

                Data[index].Read(stream);
            }

        }

        /// <summary>
        /// Writes the class fields to the stream.
        /// </summary>
        /// <param name="stream">The stream to which to write</param>
        public void Write(Stream stream)
        {
            GroupName = AuxillaryStreamingFunctions.StreamToString(stream);
            OperatorName = AuxillaryStreamingFunctions.StreamToString(stream);
            Source = AuxillaryStreamingFunctions.StreamToString(stream);
            Destination = AuxillaryStreamingFunctions.StreamToString(stream);
            DataType = AuxillaryStreamingFunctions.StreamToString(stream);
            DataCount = AuxillaryStreamingFunctions.StreamToInt(stream);
            MsgType = (MessageType) Enum.Parse(typeof (MessageType), AuxillaryStreamingFunctions.StreamToString(stream));

            foreach (var data in Data)
            {
                data.Write(stream);
            }
        }
    }
}