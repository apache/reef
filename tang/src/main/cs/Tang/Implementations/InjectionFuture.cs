using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Interface;

namespace Com.Microsoft.Tang.Implementations
{
    public class InjectionFuture
    {

        protected readonly InjectorImpl injector;
        private readonly Type iface; //entend from T
        private readonly object instance; 

        //public InjectionFuture()
        //{
        //    injector = null;
        //    iface = null;
        //    instance = null;
        //}

        public InjectionFuture(IInjector injector, Type iface) 
        {
            this.injector = (InjectorImpl)injector;
            this.iface = iface;
            this.instance = null;
        }

        public InjectionFuture(object instance)
        {
            this.injector = null;
            this.iface = null;
            this.instance = instance;
        }

        //public bool Cancel(bool mayInterruptIfRunning) 
        //{
        //    return false;
        //}

        //public bool IsCancelled()
        //{
        //    return false;
        //}

        //public bool IsDone()
        //{
        //    return true;
        //}

        public object Get() 
        {
            if(instance != null) return instance;
            try 
            {
                lock(injector) 
                {
                    object t;
                    if (typeof(Name<object>).IsAssignableFrom(iface)) 
                    {
                        t = injector.GetNamedInstance(iface);
                    } 
                    else 
                    {
                        t = injector.GetInstance(iface);
                    }
                    //Aspect a = injector.getAspect();
                    //if(a != null) 
                    //{
                    //    a.injectionFutureInstantiated(this, t);
                    //}
                    return t;
                }
            } 
            catch (InjectionException e) 
            {
                throw new Exception(e.Message);
            }
        }

    }
}
