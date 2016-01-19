// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "Clr2JavaImpl.h"

namespace Org {
  namespace Apache {
	  namespace REEF {
		  namespace Driver {
			  namespace Bridge {
				  namespace Clr2java {
					  ref class ManagedLog {
					  internal:
						  static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
					  };

					  CompletedEvaluatorClr2Java::CompletedEvaluatorClr2Java(JNIEnv *env, jobject jCompletedEvaluator) {
						  ManagedLog::LOGGER->LogStart("CompletedEvaluatorClr2Java::CompletedEvaluatorClr2Java");
						  pin_ptr<JavaVM*> pJavaVm = &_jvm;
						  if (env->GetJavaVM(pJavaVm) != 0) {
							  ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
						  }
						  _jobjectCompletedEvaluator = reinterpret_cast<jobject>(env->NewGlobalRef(jCompletedEvaluator));

						  jclass jclassCompletedEvaluator = env->GetObjectClass(_jobjectCompletedEvaluator);
						  _jstringId = CommonUtilities::GetJObjectId(env, _jobjectCompletedEvaluator, jclassCompletedEvaluator);
						  ManagedLog::LOGGER->LogStop("CompletedEvaluatorClr2Java::CompletedEvaluatorClr2Java");
					  }

					  CompletedEvaluatorClr2Java::~CompletedEvaluatorClr2Java() {
						  this->!CompletedEvaluatorClr2Java();
					  }

					  CompletedEvaluatorClr2Java::!CompletedEvaluatorClr2Java() {
						  JNIEnv *env = RetrieveEnv(_jvm);
						  if (_jobjectCompletedEvaluator != NULL) {
							  env->DeleteGlobalRef(_jobjectCompletedEvaluator);
						  }

						  if (_jstringId != NULL) {
							  env->DeleteGlobalRef(_jstringId); 
						  }
					  }

					  void CompletedEvaluatorClr2Java::OnError(String^ message) {
						  ManagedLog::LOGGER->Log("CompletedEvaluatorClr2Java::OnError");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  HandleClr2JavaError(env, message, _jobjectCompletedEvaluator);
					  }

					  String^ CompletedEvaluatorClr2Java::GetId() {
						  ManagedLog::LOGGER->Log("CompletedEvaluatorClr2Java::GetId");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  return ManagedStringFromJavaString(env, _jstringId);
					  }
				  }
			  }
		  }
	  }
  }
}