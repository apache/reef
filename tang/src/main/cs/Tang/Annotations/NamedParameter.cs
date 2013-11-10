using System;

namespace Com.Microsoft.Tang.Annotations
{
    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class NamedParameterAttribute : System.Attribute
    {
        public string Documentation { get; set; }
        public string ShortName { get; set; }
        public string DefaultValue { get; set; }
        public Type DefaultClass { get; set; }
        public string[] DefaultValues { get; set; }
        public Type[] DefaultClasses { get; set; }

        public NamedParameterAttribute(string documentation = "", string shortName = "",
            string defaultValue = "", Type defaultClass = null, string[] defaultValues = null, Type[] defaultClasses = null)
        {
            this.Documentation = documentation;
            this.ShortName = shortName;
            this.DefaultValue = defaultValue;
            this.DefaultClass = defaultClass;
            this.DefaultValues = defaultValues;
            this.DefaultClasses = defaultClasses;
        }
    }
}
