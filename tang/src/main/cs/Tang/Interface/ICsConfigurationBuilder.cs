using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Com.Microsoft.Tang.Interface
{
    public interface ICsConfigurationBuilder : IConfigurationBuilder
    {
        /// <summary>
        /// Bind named parameters, implementations or external constructors, depending
        /// on the types of the classes passed in.
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        void Bind(Type iface, Type impl);

        /// <summary>
        /// Binds the class impl as the implementation of the interface iface
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        void BindImplementation(Type iface, Type impl);

        /// <summary>
        /// Binds a value to the named parameter.
        /// </summary>
        /// <param name="name">A Type that is extended from Name<T></param>
        /// <param name="value">A string representing the value of the parameter.</param>
        void BindNamedParameter(Type name, string value);  //name must extend from Name<T>


        /// <summary>
        /// Binds the named parameter. iface must extend from Name<T> and impl extend from T
        /// </summary>
        /// <param name="iface">The iface.</param>
        /// <param name="impl">The impl.</param>
        void BindNamedParameter(Type iface, Type impl); 
    }
}
