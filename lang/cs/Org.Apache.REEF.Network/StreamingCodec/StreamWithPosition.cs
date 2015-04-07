using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.StreamingCodec
{
    public class StreamWithPosition
    {
        /// <summary>
        /// Do not Reset the stream. Start from current position
        /// </summary>
        public const long CURRENTPOSITION = -1;


        /// <summary>
        /// Create a new StreamWithPosition.
        /// </summary>
        public StreamWithPosition(Stream codecStream, long position)
        {
            CodecStream = codecStream;
            Position = position;
        }

        /// <summary>
        /// Returns the stream fron which to read and write the data.
        /// </summary>
        public Stream CodecStream { get; private set; }


        /// <summary>
        /// Returns the offset position from which to start reading
        /// This is used only for reading.
        /// </summary>
        public long Position { get; private set; }
    }
}
