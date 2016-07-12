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

            FailedTaskClr2Java::FailedTaskClr2Java(JNIEnv *env, jobject jobjectFailedTask) {
              ManagedLog::LOGGER->LogStart("FailedTaskClr2Java::AllocatedEvaluatorClr2Java");
              pin_ptr<JavaVM*> pJavaVm = &_jvm;
              if (env->GetJavaVM(pJavaVm) != 0) {
                ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
              }
              _jobjectFailedTask = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectFailedTask));
              ManagedLog::LOGGER->LogStop("FailedTaskClr2Java::AllocatedEvaluatorClr2Java");
            }

            FailedTaskClr2Java::~FailedTaskClr2Java() {
              this->!FailedTaskClr2Java();
            }

            FailedTaskClr2Java::!FailedTaskClr2Java() {
              if (_jobjectFailedTask != NULL) {
                JNIEnv *env = RetrieveEnv(_jvm);
                env->DeleteGlobalRef(_jobjectFailedTask);
              }
            }

            IActiveContextClr2Java^ FailedTaskClr2Java::GetActiveContext() {
              ManagedLog::LOGGER->LogStart("FailedTaskClr2Java::GetActiveContext");

              JNIEnv *env = RetrieveEnv(_jvm);

              jclass jclassFailedTask = env->GetObjectClass(_jobjectFailedTask);
              jmethodID jmidGetActiveContext = env->GetMethodID(jclassFailedTask, "getActiveContext", "()Lorg/apache/reef/javabridge/ActiveContextBridge;");
              jobject jobjectActiveContext = env->CallObjectMethod(_jobjectFailedTask, jmidGetActiveContext);

              ManagedLog::LOGGER->LogStop("FailedTaskClr2Java::GetActiveContext");

              if (jobjectActiveContext == NULL) {
                return nullptr;
              }

              return gcnew ActiveContextClr2Java(env, jobjectActiveContext);
            }

            array<byte>^ FailedTaskClr2Java::GetFailedTaskSerializedAvro() {
              ManagedLog::LOGGER->LogStart("FailedTaskClr2Java::GetFailedTaskSerializedAvro");
              JNIEnv *env = RetrieveEnv(_jvm);

              jclass jclassFailedTask = env->GetObjectClass(_jobjectFailedTask);
              jmethodID jmidGetFailedTaskSerializedAvro = env->GetMethodID(jclassFailedTask, "getFailedTaskSerializedAvro", "()[B");

              if (jmidGetFailedTaskSerializedAvro == NULL) {
                ManagedLog::LOGGER->LogStart("jmidGetFailedTaskSerializedAvro is NULL");
                return nullptr;
              }
              jbyteArray jFailedTaskSerializedAvro = (jbyteArray)env->CallObjectMethod(
                _jobjectFailedTask,
                jmidGetFailedTaskSerializedAvro);
              ManagedLog::LOGGER->LogStop("FailedTaskClr2Java::GetFailedTaskSerializedAvro");
              return ManagedByteArrayFromJavaByteArray(env, jFailedTaskSerializedAvro);
            }

            void FailedTaskClr2Java::OnError(String^ message) {
              ManagedLog::LOGGER->Log("FailedTaskClr2Java::OnError");
              JNIEnv *env = RetrieveEnv(_jvm);
              HandleClr2JavaError(env, message, _jobjectFailedTask);
            }
          }
        }
      }
    }
  }
}