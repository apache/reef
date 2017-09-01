# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


"""
This script creates a release candidate and relevant files.

(How to run)
python release.py <reef_home> <reef_version> <rc candidate number> <public key id (8 hex numbers)>

You can also see how to run the script with 'python release.py -h'

(Examples)
python release.py ~/reef 0.14.0 1 E488F925

"""


import subprocess
import fnmatch
import tarfile
import hashlib
import os
import argparse

"""
Get list from .gitignore
"""
def get_ignore_list(reef_home):
    f = open(reef_home + "/.gitignore", "r")
    if f is None:
        return []

    ignore_list = list()

    while True:
        line = f.readline()[:-1]
        if not line:
            break
        if "#" not in line:
            ignore_list.insert(0, "*/" + line)

    return ignore_list

"""
Make text for e-mail
"""
def get_mail_text(reef_version, rc_num):
    file_name = "apache-reef-" + reef_version + "-rc" + str(rc_num) + ".tar.gz"

    return_str = ""
    return_str += "This is to call for a new vote for the source release of Apache REEF " \
        + reef_version + " (rc" + str(rc_num) + ").\n\n"
    return_str += "The source tar ball, including signatures, digests, etc can be found at:\n" + "<yours>\n\n"
    return_str += "The Git tag is release-" + reef_version + "-rc" + str(rc_num) + "\n"
    return_str += "The Git commit ID is <Git Commit ID>\n\n\n"

    return_str += "Checksums of apache-reef-" + reef_version + "-rc" + str(rc_num) + ".tar.gz:\n\n"

    md5 = open(file_name + ".md5").read().split(" ")[0]
    return_str += "MD5: " + md5 + "\n"
    
    sha = open(file_name + ".sha512").read().split(" ")[0]
    return_str += "SHA: " + sha + "\n"

    return_str += "\nRelease artifacts are signed with a key found in the KEYS file available here:\n"
    return_str += "\nhttps://dist.apache.org/repos/dist/release/reef/KEYS\n\n\n\n"

    return_str += "<Issue Things>\n\n\n\n"

    return_str += "The vote will be open for 72 hours. Please download the release\n" \
        + "candidate, check the hashes/signature, build it and test it, and then\nplease vote:\n\n" \
        + "[ ] +1 Release this package as Apache REEF " + reef_version + "\n" \
        + "[ ] +0 no opinion\n" \
        + "[ ] -1 Do not release this package because ...\n\n" \
        + "Thanks!"

    return return_str

"""
Function to exclude files in .gitignore when making tar.gz
Return true when a file is in .gitignore
"""
def exclude_git_ignore(file_name):
    ignore_list = get_ignore_list(reef_home)
    for e in ignore_list:
        if fnmatch.fnmatch(file_name, e):
            return True

    return fnmatch.fnmatch(file_name, "*/.git") or fnmatch.fnmatch(file_name, "*/.gitignore") or \
        fnmatch.fnmatch(file_name, "*/.gitattributes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for release versioning, packaging, checksum")
    
    parser.add_argument("reef_home", type=str, help="REEF home")
    parser.add_argument("reef_version", type=str, help="REEF version")
    parser.add_argument("rc_num", type=int, help ="rc candidate number")
    parser.add_argument("key_id", type=str, help = "public key id (8 hex numbers)")
    args = parser.parse_args()
    
    reef_home = os.path.abspath(args.reef_home)
    reef_version = args.reef_version
    rc_num = args.rc_num
    key_id = args.key_id

    build_result = subprocess.call("cd " + reef_home + " && " + "mvn apache-rat:check", shell=True)

    if build_result == 0:
        file_name = "apache-reef-" + reef_version + "-rc" + str(rc_num) + ".tar.gz"

        # Make tar.gz
        tar = tarfile.open(file_name, "w:gz")
        tar.add(reef_home, arcname="apache-reef-"+reef_version, exclude=exclude_git_ignore)
        tar.close()

        gpg_str = "gpg --armor -u " + str(key_id) + " --output " + file_name + ".asc " + "--detach-sig " + file_name
        gpg_result = subprocess.call(gpg_str, shell=True)

        if gpg_result == 0:
            md5 = hashlib.md5(open(file_name, 'rb').read()).hexdigest()
            sha = hashlib.sha512(open(file_name, 'rb').read()).hexdigest()

            md5_file = open(file_name + ".md5", "w")
            md5_file.write(md5 + " *" + file_name + "\n")
            sha_file = open(file_name + ".sha512", "w")
            sha_file.write(sha + " *" + file_name + "\n")
            md5_file.close()
            sha_file.close()

            # Make a Text for an e-mail
            print "\n==================================Result==================================="
            print get_mail_text(reef_version, rc_num)
            print "===========================================================================\n"

        else:
            print "gpg error"

    else:
        print "build error - mvn apache-rat:check failed"
