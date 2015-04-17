using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.StreamingCodec
{
    /// <summary>
    /// Codec Interface that external users should implement to directly write to the stream
    /// </summary>
    public interface IStreamingCodec<T>
    {
        /// <summary>
        /// Instantiate the class from the stream.
        /// </summary>
        /// <param name="stream">The stream from which to read</param>
        ///<returns>The instance of type T read from the stream</returns>
        T Decode(Stream stream);

        /// <summary>
        /// Writes the class fields to the stream.
        /// </summary>
        /// <param name="obj">The object of type T to be encoded</param>
        /// <param name="stream">The stream to which to write</param>
        void Encode(T obj, Stream stream);
    }
}
