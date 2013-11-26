using Com.Microsoft.Tang.Annotations;

namespace com.microsoft.reef.driver.activity
{
    public class ActivityConfigurationOptions
    {
        [NamedParameter("The Identifier of the Activity", "activity", "Unnamed Activity")]
        public  class  Identifier : Name<string> { }
    }
}
