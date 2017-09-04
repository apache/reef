/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

var releaseSha512 = {
    "0.16.0": "d62c58df1f4ba962a51d81579d27321f75dad98c3c3def9bc8fb24ebf1e27978029d7ddedf26bf4ee9434cb8e6d0e4f6e1a9a4d240d03daccd9ef66bdc403f1b",
    "0.15.0": "3805103aa3d59cd23ef1edc204f8beb4cfdc92e13e4257b980463c2176301e399dd6f6c3eb9e447e94cbf407b5bfd6ec7007bcf1a87599414a2429a17ef82a75",
    "0.14.0": "8216aa9cf936a533288943245da9aab46b0df2361bcf56f757b7feff54143249dcd5bb5401498d4ecf402bfcaaf662ebbf8085322764593c055cb01d98be440a",
    "0.10.0-incubating": "53844174f701a4c0c99964260c7abb4f8ef9d93aa6b8bdddbca37082e0f5754db5b00e2ae1fdd7732735159df8205a82e7a4dde2ef5abf2ca3e5d7dc43eb62fb",
    "0.11.0-incubating": "a4741c5e8006fdca40d3f3daa43c14b82fd44e77daeb0b3539fb9705c2e9c568efd86532dce262b6d4ef7ebb240f048724b63ad7c782035a3635bebe970e1bd7",
    "0.12.0-incubating": "a5ec246fc5f73427ecb74f4725ce7ac1a8911ee7cf969aa45142b05b4985f385547b9ff47d6752e6c505dbbc98acda762d2fc22f3e2759040e2a7d9a0249398d",
    "0.13.0-incubating": "8f542aeaf2dc3b241bdcd0d343c607355e1f09e1ca89bbc3431b0cc1f0908479511f60900a91a6731051ffef8af30488eb85df567c32bc2db9d3d91014c4fed7"
};

var releaseNotes = {
    "0.16.0": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12335833",
    "0.15.0": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12334912",
    "0.14.0": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12333768",
    "0.10.0-incubating": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12329065",
    "0.11.0-incubating": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12329282",
    "0.12.0-incubating": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12332143",
    "0.13.0-incubating": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12332972"
};

function setReleaseLink() {
    var releaseVersion = this.document.getElementById("selectRelease").value;
    this.document.getElementById("listRelease").style.display = "block";

    var releaseDirectStr = "http://www.apache.org/dist/reef/" + releaseVersion + "/apache-reef-" + releaseVersion + ".tar.gz";
    this.document.getElementById("directLink").setAttribute("href", releaseDirectStr);

    var releaseMirror = "http://www.apache.org/dyn/closer.cgi/reef/" + releaseVersion;
    this.document.getElementById("mirrorLink").setAttribute("href", releaseMirror);
    this.document.getElementById("sha512Text").innerHTML = releaseSha512[releaseVersion];
    this.document.getElementById("releaseNotesLink").setAttribute("href", releaseNotes[releaseVersion]);

    var directReleaseStrSplit = releaseDirectStr.split("/");
    this.document.getElementById("directLink").innerHTML =
        directReleaseStrSplit[directReleaseStrSplit.length - 1];
    this.document.getElementById("verificationLink").setAttribute("href",
        releaseDirectStr.slice(0, (0 - directReleaseStrSplit[directReleaseStrSplit.length - 1].length)));

    var javaApiLink = "apidocs/" + releaseVersion + "/index.html";
    this.document.getElementById("javaApiLink").setAttribute("href", javaApiLink);
    if (releaseVersion.indexOf("incubating") > -1) {
        // special case: for versions 0.13.0 and earlier (incubation releases) .NET API documentation is not available
        this.document.getElementById("dotnetApiLink").innerHTML = ".NET API available since release 0.14.0";
    } else {
        var dotnetApiLink = "apidoc_net/" + releaseVersion + "/index.html";
        this.document.getElementById("dotnetApiLink").innerHTML = "<a href=" + dotnetApiLink + ">.NET API</a>";
    }
}

setReleaseLink();
