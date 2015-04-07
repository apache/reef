using System.IO;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.StreamingCodec.Impl
{
    /// <summary>This is the abstract StreamingCodec class that encodes and decodes type T. 
    /// This class is mainly built for Group Communication. This class extends inhethe DataCodec 
    /// class by having analogous streaming Encode and Decode abstract functions which are then 
    /// used to implement the memebers of IStreamingDataCodecObj interface</summary>
    public abstract class StreamingDataCodec<T> : DataCodec<T>, IStreamingDataCodecObj
    {
        /// <summary>Encodes the given object and write it to the stream</summary>
        /// <param name="obj"> the object of type object to be encoded</param>
        /// <param name="stream"> stream we want the encoding written to</param>
        public void EncodeObject(object obj, StreamWithPosition stream)
        {
            Encode((T)obj, stream);
        }

        /// <summary>Decode the data from the stream</summary>
        /// <param name="stream"> stream we want to decode from</param>
        /// <returns>The decoded object of type object</returns>
        public object DecodeObject(StreamWithPosition stream)
        {
            return Decode(stream);
        }

        /// <summary>Encodes the given object and write it to the stream</summary>
        /// <param name="obj"> the object to be encoded</param>
        /// <param name="stream"> stream we want the encoding written to</param>
        public abstract void Encode(T obj, StreamWithPosition stream);

        /// <summary>Decode the data from the stream to the message type T</summary>
        /// <param name="stream"> stream we want to decode from</param>
        /// <returns>The decoded object of type T</returns>
        public abstract T Decode(StreamWithPosition stream);
    }
}
