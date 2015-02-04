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
#Committer Guide

This captures committer-specific information beyond what's covered in the [contributions guide](contributing.html).

###Accepting Pull Requests on GitHub

The ASF INFRA team maintains [a mirror of our git repository over on GitHub](https://github.com/apache/incubator-reef).That mirror is strictly one-way: changes from the Apache git get copied over, but not the other way around. Further, the mirror on GitHub is read-only for everyone. Which means we can neither accept nor close pull requests filled there via the GitHub web UI. However, we want to allow for contributions via GitHub Pull Requests. Here's how:

####Add the ASF git repository

If you have not done so already, add the ASF git repository. For example, to add as the "apache" remote repository:

    $ git remote add apache https://git-wip-us.apache.org/repos/asf/incubator-reef.git

As a result, you can refer to the ASF git as "apache". An example setup, with a the GitHub mirror at upstream and a forked GitHub origin at `{username}`:

    $ git remote -v
    apache  https://git-wip-us.apache.org/repos/asf/incubator-reef.git (fetch)
    apache  https://git-wip-us.apache.org/repos/asf/incubator-reef.git (push)
    origin  https://github.com/{username}/incubator-reef.git (fetch)
    origin  https://github.com/{username}/incubator-reef.git (push)
    upstream  https://github.com/apache/incubator-reef.git (fetch)
    upstream  https://github.com/apache/incubator-reef.git (push)

####Merge changes

The next step is to merge all changes on the Pull Request as a single commit. There are two methods here: (A) pull the branch from the GitHub Pull Request and squash commits, or (B) get the `.diff` from GitHub and manually create a commit from this information. Each option has its pros and cons. With method A, git does some of the tedious work of preserving commit messages; but in some cases the squash may not apply cleanly, and the merger will have to carefully resolve conflicts. With method B, the `.diff` will apply cleanly (given that the branch is up-to-date, i.e. the GitHub GUI states that the "This pull request can be automatically merged by project collaborators"); but the merger will have to carefully copy over commit messages and edit author information.

####Commit Message

Whichever method you choose, the following should be included in the final commit message:

- Pull request description and commit comments
- A link to the JIRA this is addressing.
- The text "closes #PRNUMBER", where PRNUMBER is the number of the pull request, e.g. "10"

Following the last statement will close the GitHub pull request. It is important to close via the commit message, because we cannot close pull requests via the GitHub Web UI.

#####Example Commit message (80 columns)

    [REEF-33] Allow Tasks to initiate an Evaluator Heartbeat
      This adds the class HeartBeatTriggerManager which can be used by a Task to
      initiate an Evaluator Heartbeat. It is used by injecting it into the Task.
     
    JIRA:
      [REEF-33] https://issues.apache.org/jira/browse/REEF-33
     
    Pull Request:
      Closes #24
     
    Author:
      AuthorName AuthorEmail

####Method A

#####A-1. Create a branch on your local git repository

You want to make sure that that branch is current with the the master branch on Apache git:

    $ git checkout -b BRANCH_NAME
    $ git pull apache master

This assumes that you called the remote git repository at the ASF "apache". You can name the branch however you want, but it is customary to name them either after the pull request number or the JIRA id, e.g. REEF-24.

#####A-2. Pull the contribution into that branch

This is as simple as

    $ git pull GIT_REPOSITORY_OF_THE_CONTRIBUTOR BRANCH_NAME_OF_THE_CONTRIBUTION

However, the GitHub web ui makes it somewhat hard to get these two strings. The email from GitHub that informs us of Pull Requests makes this really obvious, though. Consider this quote from pull [request #10](https://github.com/apache/incubator-reef/pull/10):

>**You can merge this Pull Request by running**
>>`git pull https://github.com/jwang98052/incubator-reef juwang-logfactory`

This copies the changes from the given remote branch into the one we just created locally.

#####A-3. Check the pull request

1. Make sure the code compiles and all tests pass.
2. If the code touches code that you suspect might break on YARN or Mesos, please test on those environments. If you don't have access to a test environment, ask on the mailing list for help.
3. Make sure the code adheres to our [coding guidelines](coding-guideline.html).
4. Make sure that the additions don't create errors in a [Apache RAT](http://creadur.apache.org/rat/) check via `mvn apache-rat:check`

#####A-4. Rebase the branch onto current apache master

    # Make sure we have the latest bits
    $ git fetch apache
    # Rebase
    $ git rebase -i apache/master

In the rebase process, make sure that the contribution is squashed to a single commit. From the list of commits, "pick" the commit in the first line (the oldest), and "squash" the commits in the remaining lines:

    pick 7387a49 Comment for first commit
    squash 3371411 Comment for second commit
    squash 9bf956d Comment for third commit

[Chapter 3.6](http://www.git-scm.com/book/en/v2/Git-Branching-Rebasing) and [Chapter 7.6](http://git-scm.com/book/en/v2/Git-Tools-Rewriting-History) of the [Git Book](http://www.git-scm.com/book/en/v2) contains lots of information on what this means.

Please make sure that the commit message contains the information given in "Commit Message" above. The latest commit can be changed at any time with the command:

    $ git commit --amend

#####A-5. Push the code into apache's git

This is a good time to reflect back on this change and whether it is likely to break the build. If you are certain that it won't, go ahead and do:

    $ git checkout master
    $ git merge BRANCH_NAME
    $ git push apache master

This pushes the current branch into the master branch hosted on Apache's git repository. From there, it will be mirrored onto GitHub. And by virtue of the comment added above to the commit message, GitHub will now close the Pull Request.

####Method B

#####B-1. Update local master

In this method, you will work directly on your local master branch. Make sure you have the latest changes on your local master branch, by running:

    $ git pull apache master

#####B-2. Download the .diff and apply it

You can download the `.diff` file by appending `.diff` to the GitHub Pull Request url. This file will contain the exact changes that are shown in "Files changed" in the GitHub GUI. For example, for `https://github.com/apache/incubator-reef/pull/24` the `.diff` file is `https://github.com/apache/incubator-reef/pull/24.diff`. Using this url, run apply: 

    $ wget https://github.com/apache/incubator-reef/pull/24.diff
    $ git apply 24.diff

#####B-3. Commit and edit author information

Commit all files, making sure to include all modified and **new** files. In the commit message, make sure to include all information given in "Commit Message" above. After committing you must also change the author information to reflect the original author (and not yourself):

    $ git commit --amend --author "Original Author Name <email@address.com>"
    
#####B-4. Push the code into apache's git

Now push your single commit to apache git:

    $ git push apache master

This pushes the current branch into the master branch hosted on Apache's git repository. From there, it will be mirrored onto GitHub. And by virtue of the comment added above to the commit message, GitHub will now close the Pull Request.
