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

					  SuspendedTaskClr2Java::SuspendedTaskClr2Java(JNIEnv *env, jobject jobjectSuspendedTask) {
						  ManagedLog::LOGGER->LogStart("SuspendedTaskClr2Java::SuspendedTaskClr2Java");
						  pin_ptr<JavaVM*> pJavaVm = &_jvm;
						  if (env->GetJavaVM(pJavaVm) != 0) {
							  ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
						  }
						  _jobjectSuspendedTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectSuspendedTask));

						  jclass jclassSuspendedTask = env->GetObjectClass(_jobjectSuspendedTask);
						  _jstringId = CommonUtilities::GetJObjectId(env, _jobjectSuspendedTask, jclassSuspendedTask);
						  ManagedLog::LOGGER->LogStop("SuspendedTaskClr2Java::SuspendedTaskClr2Java");
					  }

					  IActiveContextClr2Java^ SuspendedTaskClr2Java::GetActiveContext() {
						  ManagedLog::LOGGER->LogStart("SuspendedTaskClr2Java::GetActiveContext");
						  JNIEnv *env = RetrieveEnv(_jvm);

						  jclass jclassSuspendedTask = env->GetObjectClass(_jobjectSuspendedTask);
						  jmethodID jmidGetActiveContext = env->GetMethodID(jclassSuspendedTask, "getActiveContext", "()Lorg/apache/reef/javabridge/ActiveContextBridge;");
						  jobject jobjectActiveContext = env->CallObjectMethod(_jobjectSuspendedTask, jmidGetActiveContext);
						  ManagedLog::LOGGER->LogStop("SuspendedTaskClr2Java::GetActiveContext");
						  return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
					  }

					  String^ SuspendedTaskClr2Java::GetId() {
						  ManagedLog::LOGGER->Log("SuspendedTaskClr2Java::GetId");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  return ManagedStringFromJavaString(env, _jstringId);
					  }

					  array<byte>^ SuspendedTaskClr2Java::Get() {
						  ManagedLog::LOGGER->Log("SuspendedTaskClr2Java::Get");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  jclass jclassSuspendedTask = env->GetObjectClass(_jobjectSuspendedTask);
						  jmethodID jmidGet = env->GetMethodID(jclassSuspendedTask, "get", "()[B");

						  if (jmidGet == NULL) {
							  ManagedLog::LOGGER->Log("jmidGet is NULL");
							  return nullptr;
						  }
						  jbyteArray jMessage = (jbyteArray)env->CallObjectMethod(_jobjectSuspendedTask, jmidGet);
						  return ManagedByteArrayFromJavaByteArray(env, jMessage);
					  }

					  void SuspendedTaskClr2Java::OnError(String^ message) {
						  ManagedLog::LOGGER->Log("SuspendedTaskClr2Java::OnError");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  HandleClr2JavaError(env, message, _jobjectSuspendedTask);
					  }
				  }
			  }
		  }
	  }
  }
}