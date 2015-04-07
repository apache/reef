using System.IO;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.StreamingCodec.Impl
{
    /// <summary>This is the abstract Codec class that encodes and decodes the type T. 
    /// This class is mainly built for Group Communication. This class effectively inherits 
    /// member function of ICodec<T> as abstract functions which are then used to implement 
    /// the memebers of IDataCodecObj interface</summary>
    public abstract class DataCodec<T> : IDataCodecObj, ICodec<T>
    {
        /// <summary>Encodes the given object of type object and returna byte array</summary>
        /// <param name="obj"> the object to be encoded</param>
        /// <returns>The encodng as byte array</returns>
        public byte[] EncodeObject(object obj)
        {
            return Encode((T)obj);
        }

        /// <summary>Decode the data from the byte array</summary>
        /// <param name="data"> byte array from which data is decoded</param>
        /// <returns>The decoded object of generic type object</returns>
        public object DecodeObject(byte[] data)
        {
            return Decode(data);
        }

        /// <summary>Decode the data from the byte array</summary>
        /// <param name="data"> The byte array we want to decode from</param>
        /// <returns>The decoded object of type T</returns>
        abstract public T Decode(byte[] data);

        /// <summary>Encode the data to the byte array</summary>
        /// <param name="obj"> The object of type T we want to encode</param>
        /// <returns>The encoding of the object</returns>
        public abstract byte[] Encode(T obj);
    }
}
