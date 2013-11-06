using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;
using Com.Microsoft.Tang.Implementations;

namespace Com.Microsoft.Tang.TestDriver
{
    public class TestDriver
    {
        public static void Main(string[] args)
        {
            var asm = Assembly.LoadFrom(@"Com.Microsoft.Tang.Examples.dll");
            var types = asm.GetTypes();

            Test test = new Test();
            test.Add(4);
            test.Add("avc");

            foreach (Type t in types)
            {
                System.Console.WriteLine("Full name of the type: " + t.FullName);
                System.Console.WriteLine("name of the type: " + t.Name);
                var unit = t.GetCustomAttribute<UnitAttribute>();
                var namedParameter = t.GetCustomAttribute<NamedParameterAttribute>();
                
                if (t.IsAbstract && t.IsSealed)
                    System.Console.WriteLine("this is a static" + t.FullName);


                Type argType = ClassHierarchyImpl.GetNamedParameterTargetOrNull(t);
                if (argType != null)
                {
                    System.Console.WriteLine("GetGenericArguments " + argType.Name);
                }

                Type[] intfs = t.GetInterfaces();
                foreach (Type f in intfs)
                {
                    System.Console.WriteLine("Interface: " + f.Name);
                    System.Console.WriteLine("Interface: " + f.FullName);

                    if (f.Name.Equals("Name`1"))
                    {
                        System.Console.WriteLine("XXXXXXThe class extend from Name");
                    }
                    foreach (var a in f.GetGenericArguments())
                    {
                        System.Console.WriteLine("GetGenericArguments " + a.Name);

                    }
                    ConstructorDetails(f);
                }

                System.Console.WriteLine("Constructore details");
                ConstructorDetails(t);

                Type baseT = t.BaseType;
                if (baseT != null)
                {
                    System.Console.WriteLine("base type " + baseT.FullName);
                }

                Type[] inners = t.GetNestedTypes();
                foreach (Type inn in inners)
                {
                    System.Console.WriteLine("inner class " + inn.FullName);
                }


                //get name psace
                string[] namesaces = t.FullName.Split('.');
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < namesaces.Length-1; i++ )
                {
                    sb.Append(namesaces[i]);
                    sb.Append(".");
                }
                string namesp = sb.ToString();
                System.Console.WriteLine("namespace " + namesp);
                
                //get enclosing classes full name
                string[] path = t.FullName.Split('+');
                for (int i = 1; i < path.Length; i++)
                {
                    path[i] = path[i - 1] + "." + path[i];
                }

                foreach (string s in path)
                    System.Console.WriteLine("eclosing class: " + s);
                
                if (namedParameter != null)
                {
                    System.Console.WriteLine(namedParameter);
                    System.Console.WriteLine(namedParameter.Documentation);
                    System.Console.WriteLine(namedParameter.DefaultInstance);
                }
                System.Console.WriteLine("<<<<<<");
            }

            
            Type t3 = Type.GetType(typeof(Com.Microsoft.Tang.TestDriver.TestDriver).AssemblyQualifiedName);

            //var t44 = typeof (IEnumerable<List<String>>);
            //var x101 = t44.AssemblyQualifiedName;
            
           
            Type t4 = typeof(Com.Microsoft.Tang.Examples.B);
            var t41 = t4.AssemblyQualifiedName;
            var t42 = t4.BaseType.AssemblyQualifiedName;
            System.Console.WriteLine("t4 " + t4);

            Type t411 = Type.GetType(t41);
            System.Console.WriteLine("t411 " + t411.FullName);

            Type t412 = Type.GetType(t42);
            System.Console.WriteLine("t412 " + t412.FullName);

            Type t5 = Type.GetType("Com.Microsoft.Tang.TestDriver.TestDriver");
            System.Console.WriteLine("t5 " + t5.FullName);

            Type t6 = asm.GetType("Com.Microsoft.Tang.Examples.A");
            System.Console.WriteLine("t6 " + t6);

            foreach (Type t in types)
            {
                if (t.IsInterface)
                {
                    System.Console.WriteLine(t.FullName + " is an interface: ");
                }

                if (t.IsClass)
                {
                    Type b1 = t.BaseType;
                    Type[]  infcs = t.GetInterfaces();
                    System.Console.WriteLine(t.FullName + " is a class, and its base type is : " + b1.FullName);

                    foreach (Type t2 in infcs)
                    {
                        System.Console.WriteLine(t.FullName + " is a class, and its interface  is : " + t2.FullName);
                    }

                }
            }


            System.Console.WriteLine("hello");

        }

        private static void ConstructorDetails(Type t)
        {
            var consttr = t.GetConstructors();
            foreach (ConstructorInfo ci in consttr)
            {
                System.Console.WriteLine("constructor info " + ci.ToString());
                if (ci.ContainsGenericParameters)
                {
                    var genericArg = ci.GetGenericArguments();
                    System.Console.WriteLine("Constructor genericArg : " + genericArg.Length);
                }

                foreach (var pm in ci.GetParameters())
                {
                    System.Console.WriteLine("constructor param " + pm.Name);
                }
            }
        }

        class Test : IList<int>, IList<string>
        {
            IList<int> intList = new List<int>();
            IList<string> strList = new List<string>();

            public void Add(int v)
            {
                intList.Add(v);
            }

            public void Add(string v)
            {
                strList.Add(v);
            }

            int IList<int>.IndexOf(int item)
            {
                throw new NotImplementedException();
            }

            void IList<int>.Insert(int index, int item)
            {
                throw new NotImplementedException();
            }

            void IList<int>.RemoveAt(int index)
            {
                throw new NotImplementedException();
            }

            int IList<int>.this[int index]
            {
                get
                {
                    throw new NotImplementedException();
                }
                set
                {
                    throw new NotImplementedException();
                }
            }

            void ICollection<int>.Add(int item)
            {
                throw new NotImplementedException();
            }

            void ICollection<int>.Clear()
            {
                throw new NotImplementedException();
            }

            bool ICollection<int>.Contains(int item)
            {
                throw new NotImplementedException();
            }

            void ICollection<int>.CopyTo(int[] array, int arrayIndex)
            {
                throw new NotImplementedException();
            }

            int ICollection<int>.Count
            {
                get { throw new NotImplementedException(); }
            }

            bool ICollection<int>.IsReadOnly
            {
                get { throw new NotImplementedException(); }
            }

            bool ICollection<int>.Remove(int item)
            {
                throw new NotImplementedException();
            }

            IEnumerator<int> IEnumerable<int>.GetEnumerator()
            {
                throw new NotImplementedException();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                throw new NotImplementedException();
            }

            int IList<string>.IndexOf(string item)
            {
                throw new NotImplementedException();
            }

            void IList<string>.Insert(int index, string item)
            {
                throw new NotImplementedException();
            }

            void IList<string>.RemoveAt(int index)
            {
                throw new NotImplementedException();
            }

            string IList<string>.this[int index]
            {
                get
                {
                    throw new NotImplementedException();
                }
                set
                {
                    throw new NotImplementedException();
                }
            }

            void ICollection<string>.Add(string item)
            {
                throw new NotImplementedException();
            }

            void ICollection<string>.Clear()
            {
                throw new NotImplementedException();
            }

            bool ICollection<string>.Contains(string item)
            {
                throw new NotImplementedException();
            }

            void ICollection<string>.CopyTo(string[] array, int arrayIndex)
            {
                throw new NotImplementedException();
            }

            int ICollection<string>.Count
            {
                get { throw new NotImplementedException(); }
            }

            bool ICollection<string>.IsReadOnly
            {
                get { throw new NotImplementedException(); }
            }

            bool ICollection<string>.Remove(string item)
            {
                throw new NotImplementedException();
            }

            IEnumerator<string> IEnumerable<string>.GetEnumerator()
            {
                throw new NotImplementedException();
            }
        }

    }
}
