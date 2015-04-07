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
    /// <summary>This is the Codec class that encoded and decodes to the type object 
    /// rather than some generic type T. This class is mainly built for Group Communication 
    /// GroupCommunicationMessage cannot have Generic type and hence codec of this type
    /// needs to be passed. The codecs there will implement this interface</summary>
    public interface IDataCodecObj
    {
        /// <summary>Encodes the given object of type object and returna byte array</summary>
        /// <param name="obj"> the object to be encoded</param>
        /// <returns>The encodng as byte array</returns>
        byte[] EncodeObject(object obj);

        /// <summary>Decode the data from the byte array</summary>
        /// <param name="data"> byte array from which data is decoded</param>
        /// <returns>The decoded object of generic type object</returns>
        object DecodeObject(byte[] data);
    }
}
