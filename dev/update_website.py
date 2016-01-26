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
This script updates website files with the new release information.

(How to run)
python update_website <reef_home> <reef_version> <sha512> <release_notes_link>

You can also see how to run the script with "python update_website.py -h"

(Example)
python update_website ~/reef 0.14.0 8f542ae...c4fed7 https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12333768
"""


import os
import re
import sys
import argparse
import datetime

"""
doap.rdf
"""
def update_doap(file, new_version):
    changed_str = ""
    after_release_tag = False

    f = open(file, 'r')

    # keep the part of the file before <release> tag as is
    # necessary because name and created tags appear several times in the file
    while True:
        line = f.readline()
        if not line:
            break
        if "<release>" in line:
            after_release_tag = True
        # update <name>, <revision> and <created> tags
        if (("<name>" in line) or ("<revision>" in line)) and after_release_tag:
            r = re.compile('>(.*?)<')
            m = r.search(line)
            old_version = m.group(1)
            line = line.replace(old_version, new_version)
        if "<created>" in line and after_release_tag:
            r = re.compile('<created>(.*?)</created>')
            m = r.search(line)
            old_date = m.group(1)
            new_date = datetime.date.today().strftime('%Y-%m-%d')
            line = line.replace(old_date, new_date)
        changed_str += line

    f = open(file, 'w')
    f.write(changed_str)
    f.close()
    print file


"""
downloads.md: replace all release information with new one
"""
def update_downloads(file, new_version, sha512, notes_link):
    changed_str = ""
    old_version = ""
    f = open(file, 'r')
    while True:
        line = f.readline()
        if not line:
            break
        # figure out old version
        if "selected" in line:
            r = re.compile('>(.*?)<')
            m = r.search(line)
            old_version = m.group(1)
        if old_version != "" and old_version in line:
            line = line.replace(old_version, new_version)
        if "selected" in line:
            # write this line and construct new one
            changed_str += line
            line = '    <option value="' + old_version + '">' + old_version + '</option>\n'
        if "sha512Text" in line:
            r = re.compile('<span id="sha512Text">(.*?)</span>')
            m = r.search(line)
            line = line.replace(m.group(1), sha512)
        if "releaseNotesLink" in line:
            r = re.compile('href="(.*?)"')
            m = r.search(line)
            line = line.replace(m.group(1), notes_link)
        if "dotnetApiLink" in line:
            r = re.compile('<span id="dotnetApiLink">(.*?)</span>')
            m = r.search(line)
            line = line.replace(m.group(1), '<a href="apidoc_net/' + new_version + '/index.html">.NET API</a>')
        changed_str += line

    f = open(file, 'w')
    f.write(changed_str)
    f.close()
    print file


"""
release.js: add releaseSha512 and releaseNotes for new version
"""
def update_release_js(file, new_version, sha512, notes_link):
    changed_str = ""
    f = open(file, 'r')
    while True:
        line = f.readline()
        if not line:
            break
        if "var releaseSha512" in line:
            changed_str += line
            changed_str += '    "' + new_version + '": "' + sha512 + '",\n'
            continue
        if "var releaseNotes" in line:
            changed_str += line
            changed_str += '    "' + new_version + '": "' + notes_link + '",\n'
            continue
        changed_str += line

    f = open(file, 'w')
    f.write(changed_str)
    f.close()
    print file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for updating REEF website with information about new release.")
    parser.add_argument("reef_home", type=str, help="REEF home")
    parser.add_argument("reef_version", type=str, help="REEF version")
    parser.add_argument("sha512", type=str, help="SHA512 of .tar.gz file")
    parser.add_argument("notes_link", type=str, help="Link to release notes for the version")
    args = parser.parse_args()

    reef_home = os.path.abspath(args.reef_home)
    update_doap(reef_home + "/doap.rdf", args.reef_version)
    update_release_js(reef_home + "/website/src/site/resources/js/release.js", args.reef_version, args.sha512, args.notes_link)
    update_downloads(reef_home + "/website/src/site/markdown/downloads.md", args.reef_version, args.sha512, args.notes_link)
