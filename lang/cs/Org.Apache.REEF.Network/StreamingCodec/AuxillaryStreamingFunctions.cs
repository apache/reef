using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Network.StreamingCodec
{
    /// <summary>
    /// Static class containing functions to read and write some basic types from/to stream
    /// </summary>
    public static class AuxillaryStreamingFunctions
    {
        /// <summary>
        /// Writes an integer to the stream
        /// </summary>
        /// <param name="obj">The integer to write</param>
        /// <param name="stream">The stream to write to.</param>
        public static void IntToStream(int obj, Stream stream)
        {
            stream.Write(BitConverter.GetBytes(obj), 0, sizeof(int));
        }

        /// <summary>
        /// Reads an integer from the stream
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <returns>The read integer</returns>
        public static int StreamToInt(Stream stream)
        {
            byte[] intBytes = new byte[sizeof (int)];
            stream.Read(intBytes, 0, intBytes.Length);
            return BitConverter.ToInt32(intBytes, 0);
        }

        /// <summary>
        /// Writes a string to the stream
        /// </summary>
        /// <param name="obj">The string to write</param>
        /// <param name="stream">The stream to write to.</param>
        public static void StringToStream(string obj, Stream stream)
        {
            var charString = obj.ToCharArray();
            byte[] byteString = new byte[charString.Length*sizeof (char)];
            IntToStream(byteString.Length, stream);
            Buffer.BlockCopy(charString, 0, byteString, 0, byteString.Length);
        }

        /// <summary>
        /// Reads a string from the stream
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <returns>The read string</returns>
        public static string StreamToString(Stream stream)
        {
            int length = StreamToInt(stream);
            byte[] stringByte = new byte[length];
            stream.Read(stringByte, 0, stringByte.Length);
            char[] stringChar = new char[stringByte.Length/sizeof (char)];
            Buffer.BlockCopy(stringByte, 0, stringChar, 0, stringByte.Length);
            return stringChar.ToString();
        }
    }
}
