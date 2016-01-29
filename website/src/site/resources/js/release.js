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
var releaseDirect = {
    "0.10.0-incubating": "http://www.apache.org/dist/reef/0.10.0-incubating/apache-reef-0.10.0-incubating.tar.gz",
    "0.11.0-incubating": "http://www.apache.org/dist/reef/0.11.0-incubating/apache-reef-0.11.0-incubating.tar.gz",
    "0.12.0-incubating": "http://www.apache.org/dist/reef/0.12.0-incubating/apache-reef-0.12.0-incubating.tar.gz",
    "0.13.0-incubating": "http://www.apache.org/dist/reef/0.13.0-incubating/apache-reef-0.13.0-incubating.tar.gz"
};

var releaseMirror = {
    "0.10.0-incubating": "http://www.apache.org/dyn/closer.cgi/reef/0.10.0-incubating",
    "0.11.0-incubating": "http://www.apache.org/dyn/closer.cgi/reef/0.11.0-incubating",
    "0.12.0-incubating": "http://www.apache.org/dyn/closer.cgi/reef/0.12.0-incubating",
    "0.13.0-incubating": "http://www.apache.org/dyn/closer.cgi/reef/0.13.0-incubating"
};

var releaseSha512 = {
    "0.10.0-incubating": "53844174f701a4c0c99964260c7abb4f8ef9d93aa6b8bdddbca37082e0f5754db5b00e2ae1fdd7732735159df8205a82e7a4dde2ef5abf2ca3e5d7dc43eb62fb",
    "0.11.0-incubating": "a4741c5e8006fdca40d3f3daa43c14b82fd44e77daeb0b3539fb9705c2e9c568efd86532dce262b6d4ef7ebb240f048724b63ad7c782035a3635bebe970e1bd7",
    "0.12.0-incubating": "a5ec246fc5f73427ecb74f4725ce7ac1a8911ee7cf969aa45142b05b4985f385547b9ff47d6752e6c505dbbc98acda762d2fc22f3e2759040e2a7d9a0249398d",
    "0.13.0-incubating": "8f542aeaf2dc3b241bdcd0d343c607355e1f09e1ca89bbc3431b0cc1f0908479511f60900a91a6731051ffef8af30488eb85df567c32bc2db9d3d91014c4fed7"
};

var releaseNotes = {
    "0.10.0-incubating": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12329065",
    "0.11.0-incubating": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12329282",
    "0.12.0-incubating": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12332143",
    "0.13.0-incubating": "https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&version=12332972"
};

var javaApi = {
    "0.10.0-incubating": "apidocs/0.10.0-incubating/index.html",
    "0.11.0-incubating": "apidocs/0.11.0-incubating/index.html",
    "0.12.0-incubating": "apidocs/0.12.0-incubating/index.html",
    "0.13.0-incubating": "apidocs/0.13.0-incubating/index.html"
};

var dotnetApi = {
};

function setReleaseLink() {
    var releaseVersion = this.document.getElementById("selectRelease").value;
    if (releaseDirect[releaseVersion] == undefined) {
        this.document.getElementById("listRelease").style.display= "none";
    } else {
        this.document.getElementById("listRelease").style.display = "block";

        var releaseDirectStr = releaseDirect[releaseVersion];
        this.document.getElementById("directLink").setAttribute("href", releaseDirectStr);
        this.document.getElementById("mirrorLink").setAttribute("href", releaseMirror[releaseVersion]);
        this.document.getElementById("sha512Text").innerHTML = releaseSha512[releaseVersion];
        this.document.getElementById("releaseNotesLink").setAttribute("href", releaseNotes[releaseVersion]);

        var directReleaseStrSplit = releaseDirectStr.split("/");
        this.document.getElementById("directLink").innerHTML =
            directReleaseStrSplit[directReleaseStrSplit.length - 1];
        this.document.getElementById("verificationLink").setAttribute("href",
            releaseDirectStr.slice(0, (0 - directReleaseStrSplit[directReleaseStrSplit.length - 1].length)));

        this.document.getElementById("javaApiLink").setAttribute("href", javaApi[releaseVersion]);
        if (dotnetApi[releaseVersion] == undefined) {
            // special case: for versions 0.13.0 and earlier .NET API documentation is not available
            this.document.getElementById("dotnetApiLink").innerHTML = ".NET API available since release 0.14.0";
        } else {
            this.document.getElementById("dotnetApiLink").innerHTML = "<a href=" + dotnetApi[releaseVersion] + ">.NET API</a>";
        }
    }

}
