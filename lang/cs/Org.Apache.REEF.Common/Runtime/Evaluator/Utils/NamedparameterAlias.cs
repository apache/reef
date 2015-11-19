/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Utils
{
    [NamedParameter(alias: "org.apache.reef.runtime.common.evaluator.parameters.ApplicationIdentifier", aliasLanguage: AvroConfigurationSerializer.Java)]
    public sealed class ApplicationIdentifier : Name<string>
    {
    }

    [NamedParameter(alias: "org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier", aliasLanguage: AvroConfigurationSerializer.Java)]
    public sealed class DriverRemoteIdentifier : Name<string>
    {
    }

    [NamedParameter(alias: "org.apache.reef.runtime.common.evaluator.parameters.EvaluatorIdentifier", aliasLanguage: AvroConfigurationSerializer.Java)]
    public sealed class EvaluatorIdentifier : Name<string>
    {
    }

    [NamedParameter(alias: "org.apache.reef.runtime.common.evaluator.parameters.InitialTaskConfiguration", aliasLanguage: AvroConfigurationSerializer.Java)]
    public sealed class InitialTaskConfiguration : Name<string>
    {
    }

    [NamedParameter(alias: "org.apache.reef.runtime.common.evaluator.parameters.RootContextConfiguration", aliasLanguage: AvroConfigurationSerializer.Java)]
    public sealed class RootContextConfiguration : Name<string>
    {
    }

    [NamedParameter(alias: "org.apache.reef.runtime.common.evaluator.parameters.RootServiceConfiguration", aliasLanguage: AvroConfigurationSerializer.Java)]
    public sealed class RootServiceConfiguration : Name<string>
    {
    }

    [NamedParameter(alias: "org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID", aliasLanguage: AvroConfigurationSerializer.Java)]
    public sealed class ErrorHandlerRid : Name<string>
    {
    }

    [NamedParameter(alias: "org.apache.reef.runtime.common.launch.parameters.LaunchID", aliasLanguage: AvroConfigurationSerializer.Java)]
    public sealed class LaunchId : Name<string>
    {
    }
}
