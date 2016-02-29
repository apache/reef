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
					  RunningTaskClr2Java::RunningTaskClr2Java(JNIEnv *env, jobject jobjectRunningTask) {
						  ManagedLog::LOGGER->LogStart("RunningTaskClr2Java::RunningTaskClr2Java");

						  pin_ptr<JavaVM*> pJavaVm = &_jvm;
						  if (env->GetJavaVM(pJavaVm) != 0) {
							  ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
						  }
						  _jobjectRunningTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectRunningTask));

						  jclass jclassRunningTask = env->GetObjectClass(_jobjectRunningTask);
						  _jstringId = CommonUtilities::GetJObjectId(env, _jobjectRunningTask, jclassRunningTask);
						  ManagedLog::LOGGER->LogStop("RunningTaskClr2Java::RunningTaskClr2Java");
					  }

					  RunningTaskClr2Java::~RunningTaskClr2Java() {
						  this->!RunningTaskClr2Java();
					  }

					  RunningTaskClr2Java::!RunningTaskClr2Java() {
						  JNIEnv *env = RetrieveEnv(_jvm);
						  if (_jobjectRunningTask != NULL) {
							  env->DeleteGlobalRef(_jobjectRunningTask);
						  }

						  if (_jstringId != NULL) {
							  env->DeleteGlobalRef(_jstringId);
						  }
					  }

					  IActiveContextClr2Java^ RunningTaskClr2Java::GetActiveContext() {
						  ManagedLog::LOGGER->LogStart("RunningTaskClr2Java::GetActiveContext");

						  JNIEnv *env = RetrieveEnv(_jvm);

						  jclass jclassRunningTask = env->GetObjectClass(_jobjectRunningTask);
						  jmethodID jmidGetActiveContext = env->GetMethodID(jclassRunningTask, "getActiveContext", "()Lorg/apache/reef/javabridge/ActiveContextBridge;");
						  jobject jobjectActiveContext = env->CallObjectMethod(_jobjectRunningTask, jmidGetActiveContext);
						  ManagedLog::LOGGER->LogStop("RunningTaskClr2Java::GetActiveContext");

						  return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
					  }

					  String^ RunningTaskClr2Java::GetId() {
						  ManagedLog::LOGGER->Log("RunningTaskClr2Java::GetId");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  return ManagedStringFromJavaString(env, _jstringId);
					  }

					  void RunningTaskClr2Java::Send(array<byte>^ message) {
						  ManagedLog::LOGGER->LogStart("RunningTaskClr2Java::Send");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  jclass jclassRunningTask = env->GetObjectClass(_jobjectRunningTask);
						  jmethodID jmidSend = env->GetMethodID(jclassRunningTask, "send", "([B)V");

						  if (jmidSend == NULL) {
							  ManagedLog::LOGGER->Log("jmidSend is NULL");
							  return;
						  }
						  env->CallObjectMethod(
							  _jobjectRunningTask,
							  jmidSend,
							  JavaByteArrayFromManagedByteArray(env, message));
						  ManagedLog::LOGGER->LogStop("RunningTaskClr2Java::Send");
					  }

					  void RunningTaskClr2Java::Suspend(array<byte>^ message) {
						  ManagedLog::LOGGER->LogStart("RunningTaskClr2Java::Suspend");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  jclass jclassRunningTask = env->GetObjectClass(_jobjectRunningTask);
						  jmethodID jmidSuspend = env->GetMethodID(jclassRunningTask, "suspend", "([B)V");

						  if (jmidSuspend == NULL) {
							  ManagedLog::LOGGER->Log("jmidSuspend is NULL");
							  return;
						  }
						  env->CallObjectMethod(
							  _jobjectRunningTask,
							  jmidSuspend,
							  JavaByteArrayFromManagedByteArray(env, message));
						  ManagedLog::LOGGER->LogStop("RunningTaskClr2Java::Suspend");
					  }

					  void RunningTaskClr2Java::OnError(String^ message) {
						  ManagedLog::LOGGER->Log("RunningTaskClr2Java::OnError");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  HandleClr2JavaError(env, message, _jobjectRunningTask);
					  }
				  }
			  }
		  }
	  }
  }
}