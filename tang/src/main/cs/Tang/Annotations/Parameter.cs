using System;
namespace Com.Microsoft.Tang.Annotations
{
    /// <summary>
    /// ParameterAttribute
    /// </summary>
    [System.AttributeUsage(System.AttributeTargets.Parameter)]
    public class ParameterAttribute : System.Attribute
    {
        public Type Value { get; set; } 
    }
}
