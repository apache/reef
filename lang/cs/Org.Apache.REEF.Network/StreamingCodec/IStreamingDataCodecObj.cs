using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.StreamingCodec
{
    /// <summary>This is the Streaming Codec class that encodes and decodes to the type object 
    /// rather than some generic type T. This class is mainly built for Group Communication 
    /// GroupCommunicationMessage cannot have Generic type and hence codec of this type
    /// needs to be passed. The codecs there will implement this interface</summary>
    public interface IStreamingDataCodecObj
    {
        /// <summary>Encodes the given object and write it to the stream</summary>
        /// <param name="obj"> the object of type object to be encoded</param>
        /// <param name="stream"> stream we want the encoding written to</param>
        void EncodeObject(object obj, StreamWithPosition stream);

        /// <summary>Decode the data from the stream</summary>
        /// <param name="stream"> stream we want to decode from</param>
        /// <returns>The decoded object of type object</returns>
        object DecodeObject(StreamWithPosition stream);
    }
}
