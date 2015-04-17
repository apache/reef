using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.StreamingCodec
{
    /// <summary>
    /// Writable wrapper around the user message and its codec
    /// Internally message is assumed to be of this type. The first step when the user 
    /// calls the Group operator is to take his message and codec and construct this wrapper. 
    /// </summary>
    public class MessageWrapper<T1> : IWritable
    {
        /// <summary>
        /// Returns the actual message
        /// </summary>
        public T1 Message { get; private set; }

        /// <summary>
        /// Returns the codec for the message
        /// </summary>
        public IStreamingCodec<T1> Codec { get; private set; }

        /// <summary>
        /// Constructs the message wrapper
        /// </summary>
        public MessageWrapper(T1 message, IStreamingCodec<T1> codec)
        {
            Message = message;
            Codec = codec;
        }

        /// <summary>
        /// Read the class fields from the stream.
        /// </summary>
        /// <param name="stream">The stream from which to read</param>
        /// <param name="optionalParameters">The optional parameters to be passed to the reader.
        /// For example IIdentifierFactory for NsMessage</param>
        public void Read(Stream stream, params object[] optionalParameters)
        {
            Message = Codec.Decode(stream);
        }

        /// <summary>
        /// Writes the class fields to the stream.
        /// </summary>
        /// <param name="stream">The stream to which to write</param>
        public void Write(Stream stream)
        {
            Codec.Encode(Message, stream);
        }
    }
}
