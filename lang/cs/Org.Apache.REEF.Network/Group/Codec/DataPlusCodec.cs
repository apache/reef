using System.IO;
using System.Runtime.Serialization;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Codec
{
    /// <summary>
    /// Member of StreamingGroupCommunicationMessage. Contains actual data as type object along with 
    /// the codec and stream. While encoding it, stream field is empty. While deocding, Data and 
    /// Codec field is empty. It is assumed that decoding will be done at the OperatorTopology level 
    /// since GCM does not know the Codec type while decoding.
    /// </summary>
    public class DataPlusCodec
    {
        /// <summary>
        /// Create new Data
        /// </summary>
        /// <param name="data">The actual data</param>
        /// <param name="codec">The codec for the data</param>
        /// <param name="readableStream">stream used for decoding</param>
        public DataPlusCodec(object data, IDataCodecObj codec, StreamWithPosition readableStream)
        {
            ReadableStream = readableStream;
            Data = new[] {data};
            Codec = codec;
            MessageCount = 1;
        }

        /// <summary>
        /// Create new Data
        /// </summary>
        /// <param name="data">The array of actual data</param>
        /// <param name="codec">The codec for the data</param>
        /// <param name="readableStream">stream used for decoding</param>
        public DataPlusCodec(object[] data, IDataCodecObj codec, StreamWithPosition readableStream)
        {
            Data = data;
            MessageCount = Data.Length;
            ReadableStream = readableStream;
            Codec = codec;
        }

        /// <summary>
        /// The number of messages. Used during decoding,
        /// </summary>
        [DataMember]
        public int MessageCount { get; private set; }

        /// <summary>
        /// Returns the actual message.
        /// </summary>
        [IgnoreDataMember]
        public object[] Data { get; private set; }

        /// <summary>
        /// Returns the codec.
        /// </summary>
        [IgnoreDataMember]
        public IDataCodecObj Codec { get; private set; }

        /// <summary>
        /// Returns the stream to read from.
        /// </summary>
        [IgnoreDataMember]
        public StreamWithPosition ReadableStream { get; set; }
    }
}
