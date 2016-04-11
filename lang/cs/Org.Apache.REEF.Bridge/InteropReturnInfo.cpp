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

#include "InteropReturnInfo.h"

namespace Org {
	namespace Apache {
		namespace REEF {
			namespace Bridge {
				// currently not being used
				InteropReturnInfo::InteropReturnInfo(
					JNIEnv*     env,
					jobject     jobjectInteropReturnInfo,
					ILogger^    logger
					) {
					_env = env;
					_jobjectInteropReturnInfo = jobjectInteropReturnInfo;
					jclass thisClass = env->GetObjectClass(jobjectInteropReturnInfo);
					wchar_t formatBuf[1024];

					swprintf_s(formatBuf, sizeof(formatBuf) / sizeof(wchar_t), L"zzzzzzz this should be printed by java jmid 00 %p\n", thisClass);
					logger->Log(TraceLevel::Error, gcnew String(formatBuf));
					_jmidAddExceptionString = env->GetMethodID(thisClass, "addExceptionString", "(Ljava/lang/String;)V");
					if (NULL == _jmidAddExceptionString) {
						swprintf_s(formatBuf, sizeof(formatBuf) / sizeof(wchar_t), L"_jmidAddExceptionString %p\n", _jmidAddExceptionString);
						fwprintf(stdout, formatBuf);
						fflush(stdout);
					}

					_jmidHasExceptions = env->GetMethodID(thisClass, "hasExceptions", "()Z");
					if (NULL == _jmidHasExceptions) {
						swprintf_s(formatBuf, sizeof(formatBuf) / sizeof(wchar_t), L"_jmidHasExceptions %p\n", _jmidHasExceptions);
						fwprintf(stdout, formatBuf);
						fflush(stdout);
					}

					_jmidsetReturnCode = env->GetMethodID(thisClass, "setReturnCode", "(I)V");
					if (NULL == _jmidsetReturnCode) {
						swprintf_s(formatBuf, sizeof(formatBuf) / sizeof(wchar_t), L"_jmidsetReturnCode %p\n", _jmidsetReturnCode);
						fwprintf(stdout, formatBuf);
						fflush(stdout);
					}

					_jmidgetReturnCode = env->GetMethodID(thisClass, "getReturnCode", "()I");
					if (NULL == _jmidgetReturnCode) {
						swprintf_s(formatBuf, sizeof(formatBuf) / sizeof(wchar_t), L"_jmidgetReturnCode %p\n", _jmidgetReturnCode);
						fwprintf(stdout, formatBuf);
						fflush(stdout);
					}
				}

				void InteropReturnInfo::AddExceptionString(String^ exceptionString) {
					HasExceptions();
					pin_ptr<const wchar_t> wch = PtrToStringChars(exceptionString);
					jstring ret = _env->NewString((const jchar*)wch, exceptionString->Length);
					_env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidAddExceptionString, ret);
					HasExceptions();
				}

				Boolean InteropReturnInfo::HasExceptions() {
					jobject obj = _env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidHasExceptions);
					return obj != NULL;
				}
				void InteropReturnInfo::SetReturnCode(int rc) {
					_env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidsetReturnCode, rc);
					GetReturnCode();
				}
				int InteropReturnInfo::GetReturnCode() {
					jobject obj = _env->CallObjectMethod(_jobjectInteropReturnInfo, _jmidgetReturnCode);
					return (int)obj;
				}
			}
		}
	}
}