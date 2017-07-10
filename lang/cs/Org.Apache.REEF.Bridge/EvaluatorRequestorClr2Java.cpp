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

            EvaluatorRequestorClr2Java::EvaluatorRequestorClr2Java(JNIEnv *env, jobject jevaluatorRequestor) {
              ManagedLog::LOGGER->LogStart("EvaluatorRequestorClr2Java::EvaluatorRequestorClr2Java");
              pin_ptr<JavaVM*> pJavaVm = &_jvm;
              if (env->GetJavaVM(pJavaVm) != 0) {
                ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
              }
              _jobjectEvaluatorRequestor = reinterpret_cast<jobject>(env->NewGlobalRef(jevaluatorRequestor));
              ManagedLog::LOGGER->LogStop("EvaluatorRequestorClr2Java::EvaluatorRequestorClr2Java");
            }

            EvaluatorRequestorClr2Java::~EvaluatorRequestorClr2Java() {
              this->!EvaluatorRequestorClr2Java();
            }

            EvaluatorRequestorClr2Java::!EvaluatorRequestorClr2Java() {
              if (_jobjectEvaluatorRequestor != NULL) {
                JNIEnv *env = RetrieveEnv(_jvm);
                env->DeleteGlobalRef(_jobjectEvaluatorRequestor);
              }
            }

            void EvaluatorRequestorClr2Java::Submit(IEvaluatorRequest^ request) {
              ManagedLog::LOGGER->LogStart("EvaluatorRequestorClr2Java::Submit");
              JNIEnv *env = RetrieveEnv(_jvm);
              jclass jclassEvaluatorRequestor = env->GetObjectClass(_jobjectEvaluatorRequestor);
              jmethodID jmidSubmit = env->GetMethodID(jclassEvaluatorRequestor, "submit", "(IIIZLjava/lang/String;Ljava/lang/String;Ljava/util/ArrayList;)V");

              if (jmidSubmit == NULL) {
                fprintf(stdout, " jmidSubmit is NULL\n");
                fflush(stdout);
                return;
              }
              env->CallObjectMethod(
                _jobjectEvaluatorRequestor,
                jmidSubmit,
                request->Number,
                request->MemoryMegaBytes,
                request->VirtualCore,
                request->RelaxLocality,
                JavaStringFromManagedString(env, request->Rack),
                JavaStringFromManagedString(env, request->RuntimeName),
                JavaArrayListFromManagedList(env, request->NodeNames));
              ManagedLog::LOGGER->LogStop("EvaluatorRequestorClr2Java::Submit");
            }

            array<byte>^ EvaluatorRequestorClr2Java::GetDefinedRuntimes() {
              ManagedLog::LOGGER->LogStart("EvaluatorRequestorClr2Java::GetDefinedRuntimes");
              JNIEnv *env = RetrieveEnv(_jvm);
              jclass jclassEvaluatorRequestor = env->GetObjectClass(_jobjectEvaluatorRequestor);
              jmethodID jmidGetDefinedRuntimes = env->GetMethodID(jclassEvaluatorRequestor, "getDefinedRuntimes", "()[B");

              if (jmidGetDefinedRuntimes == NULL) {
                fprintf(stdout, " jmidGetDefinedRuntimes is NULL\n");
                fflush(stdout);
                return nullptr;
              }

              jbyteArray jBytes = (jbyteArray)env->CallObjectMethod(
                _jobjectEvaluatorRequestor,
                jmidGetDefinedRuntimes);

              ManagedLog::LOGGER->LogStop("EvaluatorRequestorClr2Java::GetDefinedRuntimes");
              return ManagedByteArrayFromJavaByteArray(env, jBytes);
            }

            void EvaluatorRequestorClr2Java::OnError(String^ message) {
              ManagedLog::LOGGER->Log("EvaluatorRequestorClr2Java::OnError");
              JNIEnv *env = RetrieveEnv(_jvm);
              HandleClr2JavaError(env, message, _jobjectEvaluatorRequestor);
            }
          }
        }
      }
    }
  }
}