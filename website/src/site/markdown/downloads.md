<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
#Downloads

##Releases

Choose a release version:
<select id="selectRelease" onchange="setReleaseLink()">
    <option value="0.13.0-incubating" selected="selected">0.13.0-incubating</option>
    <option value="0.12.0-incubating">0.12.0-incubating</option>
    <option value="0.11.0-incubating">0.11.0-incubating</option>
    <option value="0.10.0-incubating">0.10.0-incubating</option>
</select>

<ul id="listRelease">
    <li>
        Download directly:
        <a id="directLink" href="http://www.apache.org/dist/incubator/reef/0.13.0-incubating/apache-reef-0.13.0-incubating.tar.gz">
            apache-reef-0.13.0-incubating.tar.gz
        </a>
    </li>
    <li>
        Download from mirror:
        <a id="mirrorLink" href="http://www.apache.org/dyn/closer.cgi/incubator/reef/0.13.0-incubating">
        Closest Apache Mirror</a>
    </li>
    <li>
        Verification:
        <a id="verificationLink" href="http://www.apache.org/dist/incubator/reef/0.13.0-incubating/">
            Signatures and checksums
        </a>
        <br />
        SHA512: <span id="sha512Text">8f542aeaf2dc3b241bdcd0d343c607355e1f09e1ca89bbc3431b0cc1f0908479511f60900a91a6731051ffef8af30488eb85df567c32bc2db9d3d91014c4fed7</span>
    </li>
    <li>
        <a id="releaseNotesLink" href="http://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315820&amp;version=12332972">Release notes</a>
    </li>
</ul>

##Development and Maintenance Branches

If you are interested in working with the newest under-development code or contributing to REEF, you can also check out the master branch from Git:

    $ git clone git://git.apache.org/incubator-reef.git

##How to verify the integrity of the files

It is essential that you verify the integrity of the downloaded files using the PGP or MD5 signatures. Please read [Verifying Apache HTTP Server Releases](http://www.apache.org/info/verification.html) for more information on why you should verify our releases.

The PGP signatures can be verified using [PGP](http://www.pgpi.org/) or [GPG](https://www.gnupg.org/). First download the [KEYS](http://www.apache.org/dist/incubator/reef/KEYS) as well as the `*.asc` signature file for the relevant distribution. Make sure you get these files from the [main distribution directory](http://www.apache.org/dist/incubator/reef/) rather than from a mirror. Then verify the signatures using one of the following sets of commands:

    % pgpk -a KEYS
    % pgpv downloaded_file.asc

or

    % pgp -ka KEYS
    % pgp downloaded_file.asc

or

    % gpg --import KEYS
    % gpg --verify downloaded_file.asc

Alternatively, you can verify the MD5 signature on the files. A Unix/Linux program called md5 or md5sum is included in most distributions. It is also available as part of [GNU Textutils](http://www.gnu.org/software/textutils/textutils.html). Windows users can get binary md5 programs from these (and likely other) places: 


- http://www.md5summer.org/
- http://www.fourmilab.ch/md5/
- http://www.pc-tools.net/win32/md5sums/

##Maven Dependencies

REEF artifacts are hosted in [Maven Central](http://search.maven.org/#search|ga|1|org.apache.reef) and can be added to your Maven project using the following format:

    <dependency>
        <groupId>org.apache.reef</groupId>
        <artifactId>reef-project</artifactId>
        <version>{$REEF_VERSION}</version>
    </dependency>
 
