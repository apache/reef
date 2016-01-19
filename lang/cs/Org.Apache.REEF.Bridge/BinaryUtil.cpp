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

#include "InteropUtil.h"
#include "BinaryUtil.h"

DWORD GetActualAddressFromRVA(IMAGE_SECTION_HEADER* pSectionHeader, IMAGE_NT_HEADERS* pNTHeaders, DWORD dwRVA) {
  DWORD dwRet = 0;

  for (int j = 0; j < pNTHeaders->FileHeader.NumberOfSections; j++, pSectionHeader++) {
    DWORD cbMaxOnDisk = min( pSectionHeader->Misc.VirtualSize, pSectionHeader->SizeOfRawData );

    DWORD startSectRVA, endSectRVA;

    startSectRVA = pSectionHeader->VirtualAddress;
    endSectRVA = startSectRVA + cbMaxOnDisk;

    if ( (dwRVA >= startSectRVA) && (dwRVA < endSectRVA)) {
      dwRet =  (pSectionHeader->PointerToRawData ) + (dwRVA - startSectRVA);
      break;
    }

  }

  return dwRet;
}


extern "C" __declspec(dllexport) BINARY_TYPE __stdcall IsManagedBinary(const wchar_t*  lpszImageName) {
  BINARY_TYPE binaryType = BINARY_TYPE_NONE;
  HANDLE hFile = CreateFile(lpszImageName, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);

  if (INVALID_HANDLE_VALUE != hFile) {
    //succeeded
    HANDLE hOpenFileMapping = CreateFileMapping(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
    if (hOpenFileMapping) {
      BYTE* lpBaseAddress = NULL;

      lpBaseAddress = (BYTE*)MapViewOfFile(hOpenFileMapping, FILE_MAP_READ, 0, 0, 0);

      if (lpBaseAddress) {
        //having mapped the executable to our process space, now start navigating through the sections

        //DOS header is straightforward. It is the topmost structure in the PE file
        //i.e. the one at the lowest offset into the file
        IMAGE_DOS_HEADER* pDOSHeader = (IMAGE_DOS_HEADER*)lpBaseAddress;

        //the only important data in the DOS header is the e_lfanew
        //the e_lfanew points to the offset of the beginning of NT Headers data
        IMAGE_NT_HEADERS* pNTHeaders = (IMAGE_NT_HEADERS*)((BYTE*)pDOSHeader + pDOSHeader->e_lfanew);

        IMAGE_SECTION_HEADER* pSectionHeader = (IMAGE_SECTION_HEADER*)((BYTE*)pNTHeaders + sizeof(IMAGE_NT_HEADERS));

        //Now, start parsing
        //check if it is a PE file

        if (pNTHeaders->Signature == IMAGE_NT_SIGNATURE) {
          //start parsing COM table

          DWORD dwNETHeaderTableLocation = pNTHeaders->OptionalHeader.DataDirectory[IMAGE_DIRECTORY_ENTRY_COM_DESCRIPTOR].VirtualAddress;

          if (dwNETHeaderTableLocation) {
            //import data does exist for this module
            IMAGE_COR20_HEADER* pNETHeader = (IMAGE_COR20_HEADER*)((BYTE*)pDOSHeader + GetActualAddressFromRVA(pSectionHeader, pNTHeaders, dwNETHeaderTableLocation));

            if (pNETHeader) {
              binaryType = BINARY_TYPE_CLR;
            }
            else {
              binaryType = BINARY_TYPE_NATIVE;
            }
          }
          else {
            binaryType = BINARY_TYPE_NATIVE;
          }
        }
        else {
          binaryType = BINARY_TYPE_NONE;
        }
        UnmapViewOfFile(lpBaseAddress);
      }
      CloseHandle(hOpenFileMapping);
    }
    CloseHandle(hFile);
  }
  return binaryType;
}
