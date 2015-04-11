using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.StreamingCodec
{
    /// <summary>
    /// Interface that classes should implement if they need to be readable to and writable 
    /// from the stream. It is assumed that the classes inheriting this interface will have a 
    /// default empty constructor</summary>
    public interface IWritable
    {
        /// <summary>
        /// Read the class fields from the stream.
        /// </summary>
        /// <param name="stream">The stream from which to read</param>
        /// <param name="optionalParameters">The optional parameters to be passed to the reader.
        /// For example IIdentifierFactory for NsMessage</param>
        void Read(Stream stream, params object[] optionalParameters);

        /// <summary>
        /// Writes the class fields to the stream.
        /// </summary>
        /// <param name="stream">The stream to which to write</param>
        void Write(Stream stream);
    }
}
