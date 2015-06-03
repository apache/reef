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
    "0.10.0-incubating": "http://www.apache.org/dist/incubator/reef/0.10.0-incubating/apache-reef-0.10.0-incubating.tar.gz",
    "0.11.0-incubating": "http://www.apache.org/dist/incubator/reef/0.11.0-incubating/apache-reef-0.11.0-incubating.tar.gz"
};

var releaseMirror = {
    "0.10.0-incubating": "http://www.apache.org/dyn/closer.cgi/incubator/reef/0.10.0-incubating",
    "0.11.0-incubating": "http://www.apache.org/dyn/closer.cgi/incubator/reef/0.11.0-incubating"
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

        var directReleaseStrSplit = releaseDirectStr.split("/");
        this.document.getElementById("directLink").innerHTML =
            directReleaseStrSplit[directReleaseStrSplit.length - 1];

    }

}
