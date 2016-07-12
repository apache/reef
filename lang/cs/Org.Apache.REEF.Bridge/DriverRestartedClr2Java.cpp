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

            DriverRestartedClr2Java::DriverRestartedClr2Java(JNIEnv *env, jobject jdriverRestarted) {
              ManagedLog::LOGGER->LogStart("DriverRestartedClr2Java::DriverRestartedClr2Java");
              pin_ptr<JavaVM*> pJavaVm = &_jvm;
              if (env->GetJavaVM(pJavaVm) != 0) {
                ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
              }
              _jobjectDriverRestarted = reinterpret_cast<jobject>(env->NewGlobalRef(jdriverRestarted));

              jclass jclassDriverRestarted = env->GetObjectClass(_jobjectDriverRestarted);
              jmethodID jmidGetExpectedEvaluatorIds = env->GetMethodID(jclassDriverRestarted, "getExpectedEvaluatorIds", "()[Ljava/lang/String;");
              jmethodID jmidGetResubmissionAttempts = env->GetMethodID(jclassDriverRestarted, "getResubmissionAttempts", "()I");

              _resubmissionAttempts = env->CallIntMethod(_jobjectDriverRestarted, jmidGetResubmissionAttempts);
              jobjectArray jevaluatorIds = CommonUtilities::CallGetMethodNewGlobalRef<jobjectArray>(env, _jobjectDriverRestarted, jmidGetExpectedEvaluatorIds);
              _startTime = System::DateTime::Now;
              int count = env->GetArrayLength(jevaluatorIds);
              _expectedEvaluatorIds = gcnew array<String^>(count);

              for (int i = 0; i < count; i++) {
                jstring string = (jstring)(*env).GetObjectArrayElement(jevaluatorIds, i);
                _expectedEvaluatorIds[i] = ManagedStringFromJavaString(env, string);
              }

              env->DeleteGlobalRef(jevaluatorIds);

              ManagedLog::LOGGER->LogStop("DriverRestartedClr2Java::DriverRestartedClr2Java");
            }

            DriverRestartedClr2Java::~DriverRestartedClr2Java() {
              this->!DriverRestartedClr2Java();
            }

            DriverRestartedClr2Java::!DriverRestartedClr2Java() {
              if (_jobjectDriverRestarted != NULL) {
                JNIEnv *env = RetrieveEnv(_jvm);
                env->DeleteGlobalRef(_jobjectDriverRestarted);
              }
            }

            array<String^>^ DriverRestartedClr2Java::GetExpectedEvaluatorIds() {
              return _expectedEvaluatorIds;
            }

            DateTime DriverRestartedClr2Java::GetStartTime() {
              return _startTime;
            }

            int DriverRestartedClr2Java::GetResubmissionAttempts() {
              return _resubmissionAttempts;
            }

            void DriverRestartedClr2Java::OnError(String^ message) {
              ManagedLog::LOGGER->Log("DriverRestartedClr2Java::OnError");
              JNIEnv *env = RetrieveEnv(_jvm);
              HandleClr2JavaError(env, message, _jobjectDriverRestarted);
            }
          }
        }
      }
    }
  }
}