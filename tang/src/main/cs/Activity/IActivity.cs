using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace com.microsoft.reef.activity
{
   public interface IActivity
    {
       byte[] Call(byte[] memento);
    }
}
