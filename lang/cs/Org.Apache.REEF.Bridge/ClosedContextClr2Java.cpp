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
                        ClosedContextClr2Java::ClosedContextClr2Java(JNIEnv *env, jobject jobjectClosedContext) {
                            ManagedLog::LOGGER->LogStart("ClosedContextClr2Java::ClosedContextClr2Java");

                            pin_ptr<JavaVM*> pJavaVm = &_jvm;
                            if (env->GetJavaVM(pJavaVm) != 0) {
                                ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
                            }

                            // Gets the Java closed context class to get its fields
                            _jobjectClosedContext = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectClosedContext));
                            jclass jclassClosedContext = env->GetObjectClass(_jobjectClosedContext);

                            // Gets the Context ID and Evaluator ID of the closed context from the Java closed context object.
                            _jstringContextId = CommonUtilities::GetJObjectId(env, _jobjectClosedContext, jclassClosedContext);
                            _jstringEvaluatorId = CommonUtilities::GetJObjectEvaluatorId(env, _jobjectClosedContext, jclassClosedContext);
                            
                            jmethodID jmidGetParentContextMid = env->GetMethodID(
                                jclassClosedContext, "getParentContextBridge", "()Lorg/apache/reef/javabridge/ActiveContextBridge;");

                            jobject parentContext = env->CallObjectMethod(_jobjectClosedContext, jmidGetParentContextMid);

                            _parentContext = gcnew ActiveContextClr2Java(env, parentContext);

                            ManagedLog::LOGGER->LogStop("ClosedContextClr2Java::ClosedContextClr2Java");
                        }

                        ClosedContextClr2Java::~ClosedContextClr2Java() {
                            this->!ClosedContextClr2Java();
                        }

                        ClosedContextClr2Java::!ClosedContextClr2Java() {
                            JNIEnv *env = RetrieveEnv(_jvm);
                            if (_jobjectClosedContext != NULL) {
                                env->DeleteGlobalRef(_jobjectClosedContext);
                            }
                            
                            if (_jstringContextId != NULL) {
                                env->DeleteGlobalRef(_jstringContextId);
                            }

                            if (_jstringEvaluatorId != NULL) {
                                env->DeleteGlobalRef(_jstringEvaluatorId);
                            }
                        }

                        /**
                         * Gets the Parent context of the closed context through a JNI call to Java.
                         */
                        IActiveContextClr2Java^ ClosedContextClr2Java::GetParentContext() {
                            return _parentContext;
                        }

                        /**
                         * Gets the ID of the closed context from Java.
                         */
                        String^ ClosedContextClr2Java::GetId() {
                            ManagedLog::LOGGER->Log("ClosedContextClr2Java::GetId");
                            JNIEnv *env = RetrieveEnv(_jvm);
                            return ManagedStringFromJavaString(env, _jstringContextId);
                        }

                        /**
                         * Gets the Evaluator ID of the Evaluator on which the Closed Context was active.
                         */
                        String^ ClosedContextClr2Java::GetEvaluatorId() {
                            ManagedLog::LOGGER->Log("ClosedContextClr2Java::GetEvaluatorId");
                            JNIEnv *env = RetrieveEnv(_jvm);
                            return ManagedStringFromJavaString(env, _jstringEvaluatorId);
                        }

                        /**
                         * Gets the EvaluatorDescriptor of the Evaluator on which the Closed Context was active.
                         */
                        IEvaluatorDescriptor^ ClosedContextClr2Java::GetEvaluatorDescriptor() {
                            ManagedLog::LOGGER->LogStart("ClosedContextClr2Java::GetEvaluatorDescriptor");
                            return CommonUtilities::RetrieveEvaluatorDescriptor(_jobjectClosedContext, _jvm);
                        }

                        void ClosedContextClr2Java::OnError(String^ message) {
                            ManagedLog::LOGGER->Log("ClosedContextClr2Java::OnError");
                            JNIEnv *env = RetrieveEnv(_jvm);
                            HandleClr2JavaError(env, message, _jobjectClosedContext);
                        }
                    }
                }
            }
        }
    }
}