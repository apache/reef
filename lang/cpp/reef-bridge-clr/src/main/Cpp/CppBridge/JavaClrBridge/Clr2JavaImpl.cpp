/**
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
#include "Clr2JavaImpl.h"

namespace Microsoft
{
	namespace Reef
	{
		namespace Interop
		{
			Clr2JavaImpl::Clr2JavaImpl (JNIEnv* env, jobject  jobjectEManager, jobject  jobjectDriverManager)
			{
				_env = env;
				_jobjectEManager = jobjectEManager;
				_jclassEManager =  env->GetObjectClass (_jobjectEManager);
				_jmidSubmit = env->GetMethodID(_jclassEManager, "submit", "([B)V");	
				_jobjectDriverManager = jobjectDriverManager;
				_jclassDriverManager = env->GetObjectClass (_jobjectDriverManager);

			}

			void Clr2JavaImpl::AllocatedEvaluatorSubmitContextAndTask(String^ contextConfigStr, String^ taskConfigStr)
			{
				Console::WriteLine("AllocatedEvaluatorSubmitContextAndTask not implemented for Clr2JavaImpl");
			}
		}
	}
}