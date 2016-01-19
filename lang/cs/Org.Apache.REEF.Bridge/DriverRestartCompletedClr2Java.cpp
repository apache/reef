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

						DriverRestartCompletedClr2Java::DriverRestartCompletedClr2Java(JNIEnv *env, jobject jdriverRestartCompleted) {
							ManagedLog::LOGGER->LogStart("DriverRestartCompletedClr2Java::DriverRestartCompletedClr2Java");
							pin_ptr<JavaVM*> pJavaVm = &_jvm;
							if (env->GetJavaVM(pJavaVm) != 0) {
								ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
							}
							_jobjectDriverRestartCompleted = reinterpret_cast<jobject>(env->NewGlobalRef(jdriverRestartCompleted));

							jclass jclassDriverRestartCompleted = env->GetObjectClass(_jobjectDriverRestartCompleted);
							jmethodID jmidIsTimedOut = env->GetMethodID(jclassDriverRestartCompleted, "isTimedOut", "()Z");

							jboolean jisTimedOut = env->CallBooleanMethod(_jobjectDriverRestartCompleted, jmidIsTimedOut);
							_restartCompletedTime = System::DateTime::Now;
							_isTimedOut = ClrBoolFromJavaBoolean(env, jisTimedOut);

							ManagedLog::LOGGER->LogStop("DriverRestartCompletedClr2Java::DriverRestartCompletedClr2Java");
						}

						DriverRestartCompletedClr2Java::~DriverRestartCompletedClr2Java() {
							this->!DriverRestartCompletedClr2Java();
						}

						DriverRestartCompletedClr2Java::!DriverRestartCompletedClr2Java() {
							if (_jobjectDriverRestartCompleted != NULL) {
								JNIEnv *env = RetrieveEnv(_jvm);
								env->DeleteGlobalRef(_jobjectDriverRestartCompleted);
							}
						}

						bool DriverRestartCompletedClr2Java::IsTimedOut() {
							return _isTimedOut;
						}

						DateTime DriverRestartCompletedClr2Java::GetCompletedTime() {
							return _restartCompletedTime;
						}

						void DriverRestartCompletedClr2Java::OnError(String^ message) {
							ManagedLog::LOGGER->Log("DriverRestartCompletedClr2Java::OnError");
							JNIEnv *env = RetrieveEnv(_jvm);
							HandleClr2JavaError(env, message, _jobjectDriverRestartCompleted);
						}
					}
				}
			}
		}
	}
}