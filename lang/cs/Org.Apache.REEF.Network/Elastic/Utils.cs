using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Utilities.Attributes;
using System;
using System.Globalization;

namespace Org.Apache.REEF.Network.Elastic
{
    /// <summary>
    /// Utility class.
    /// </summary>
    [Unstable("0.16", "Class may change or be removed")]
    public static class Utils
    {
        /// <summary>
        /// Builds a task identifier out of a subscription(s) and an id.
        /// </summary>
        /// <param name="subscriptionName">The subscriptions active in the task</param>
        /// <param name="id">The task id</param>
        /// <returns>The task identifier</returns>
        public static string BuildTaskId(string subscriptionName, int id)
        {
            return BuildIdentifier("Task", subscriptionName, id);
        }

        /// <summary>
        /// Utility method returning an identifier by merging the input fields
        /// </summary>
        /// <param name="first">The first field</param>
        /// <param name="second">The second field</param>
        /// <param name="third">The third field</param>
        /// <returns>An id merging the three fields</returns>
        private static string BuildIdentifier(string first, string second, int third)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", first, second, third);
        }
    }
}