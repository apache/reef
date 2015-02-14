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
#Contributing to REEF

####First things first: Welcome!

Arriving on this page means that you are interested in helping us out. For that: Welcome and thank you! REEF is a community driven project and we always welcome new people joining us. We are inviting contributions in many forms to all parts of the project, including:

- Bug reports regarding the software, the documentation, the website, guides like this, etc.
- Graphics (for instance, [we don't have a logo yet](https://issues.apache.org/jira/browse/REEF-14)).
- Documentation updates, tutorials, examples.
- Code: Patches, new features, etc

####Getting started: Join the community.

The first step is to **join the community** by joining our [mailing list](mailing-list.html). This is where all discussions regarding the project are happening, where we answer each other's questions and where you will find a friendly bunch to welcome you. 

If you want to work on the REEF code base, it is a good idea to [learn how to compile and test REEF](tutorial.html). 

####Finding things to work on

At any given time, there is any number of [open, unassigned issues on REEF](https://issues.apache.org/jira/issues/?jql=project%20%3D%20REEF%20AND%20status%20%3D%20Open%20AND%20resolution%20%3D%20Unresolved%20AND%20assignee%20in%20\(EMPTY\)%20ORDER%20BY%20priority%20DESC). Note that that list doesn't only contain coding tasks, but also documentation, graphics work, the website and so on. We use JIRA to manage all open todos for the project, software or otherwise. However, some of the items on that list might since have become obsolete or such. Hence, it is always a good idea to get in touch with the rest of the community on the mailing list before getting started.

####Code contribution process

While contributing to REEF, you will encounter ASF infrastructure, GitHub infrastructure, and follow ASF and REEF-specific customs. The most important of those customs is to communicate throughout the whole process. That way, the REEF community has a chance to help you along the way.

####Before you start the work

An important part of the process comes before you start the contribution process. Most issues should first be brought up in the [dev@reef.incubator.apache.org](mailto:dev@reef.incubator.apache.org) mailing list. If you haven't done so yet, [subscribe to the list](mailing-list.html). After discussion, you or one of the other developers will create an Issue on [ASF JIRA](https://issues.apache.org/jira/browse/REEF/). Again, create an account if you don't have one. Write a succinct description of the Issue, making sure to incorporate any discussion from the mailing list.

And once you are ready to make changes, have a look at the [coding guidelines](coding-guideline.html) to make sure your code agrees with the rest of REEF.

####Creating the contribution as a GitHub Pull Request

REEF uses [GitHub's Pull Requests](https://help.github.com/articles/using-pull-requests/) as the main method to accept changes: you fork REEF into your own repository, make changes, and then send a pull request to the GitHub repository. In more detail:

1. Fork repository
2. Create branch
3. Make changes in your local machine
4. Merge the master branch into your branch
5. Push commits into the your remote repo (forked)
6. Send a pull request.
7. Participate in the code review.

#####1. Fork repository

First, you need to fork the REEF repository. Go to the [Github repository](https://github.com/apache/incubator-reef) mirrored from ASF, and click "Fork" button. Then you will have your own repository. Clone this repository to your local machine:

    $ git clone https://github.com/{your_alias}/incubator-reef.git

Then, add the Apache GitHub repository as upstream:

    $ git remote add upstream https://github.com/apache/incubator-reef

A correct git configuration should look similar to the following, with an origin and upstream:

    $ git remote -v
    origin https://github.com/{your_alias}/incubator-reef.git (fetch)
    origin https://github.com/{your_alias}/incubator-reef.git (push)
    upstream https://github.com/apache/incubator-reef.git (fetch)
    upstream https://github.com/apache/incubator-reef.git (push)

If you have an `apache.org` email address, now is the time to [configure git](https://git-wip-us.apache.org/) to use it:

    $ git config user.name "My Name Here"
    $ git config user.email myusername@apache.org

#####2. Create a branch to work on
Before making changes, you have to make sure the issue to resolve (e.g. fix a bug, implement a new feature, etc) is registered in the [REEF JIRA](https://issues.apache.org/jira/browse/REEF). Create a branch to address the issue on your own. The name of the branch should reference the issue, e.g., `REEF-{issue_number}`. You can take a look how others name their branches.

#####3. Make changes in your local machine
Write the code and make commits as usual. Make sure all new files contain the [ASF licensing header](https://github.com/apache/incubator-reef/blob/master/LICENSE_HEADER.txt).

#####4. Merge the master branch into your branch
Before sending a pull request, you should make sure that your branch includes the latest changes in the master branch. Please run the following:

    $ git fetch upstream
    $ git checkout {your_branch} # Skip this step if you are already on your branch.
    $ git merge upstream/master

Resolve the conflicts if they exist. Test with the merged code so that it does not break the system. Then, check that Apache headers are in place where needed, by running RAT:

    $ mvn apache-rat:check

Finally, as a courtesy to the merger, you can rebase to master and squash all the commits from your PR into one:

    # Rebase
    $ git rebase -i upstream/master

In the rebase process, make sure that the contribution is squashed to a single commit. From the list of commits, "pick" the commit in the first line (the oldest), and "squash" the commits in the remaining lines:

    pick   7387a49 Comment for first commit
    squash 3371411 Comment for second commit
    squash 9bf956d Comment for third commit

[Chapter 3.6](http://www.git-scm.com/book/en/v2/Git-Branching-Rebasing) and [Chapter 7.6](http://git-scm.com/book/en/v2/Git-Tools-Rewriting-History) of the [Git Book](http://www.git-scm.com/book/en/v2) contains lots of information on what this means. 

In this process, git allows you to edit the commit message for the final, squashed commit. This commit message will serve as the description of the pull request and will in all likelihood appear in verbatim in the REEF history. In other words: Spend some time making it good. A common template for this commit message is:

    [REEF-JIRA_ISSUE_NUMBER]: THE_TITLE_OF_THE_JIRA
     
    This addressed the issue by 
      * INSERT_DESCRIPTION_OF_SOMETHING
      * INSERT_DESCRIPTION_OF_SOMETHING_ELSE
      * ...
     
    JIRA: [REEF-JIRA_ISSUE_NUMBER](https://issues.apache.org/jira/browse/REEF-JIRA_ISSUE_NUMBER)

You can get a good idea how other people write their commit messages by inspecting the output of `git log`.

#####5. Push commits into the your remote repo (forked)

You're almost done! Push your commits into your own repo.
    
    $ git push origin HEAD

#####6. Send a pull request

It is time to send a pull request. If you do not know much about pull requests, you may want to read this [article](https://help.github.com/articles/using-pull-requests/).

If you go to the repository at the Github website, you can notice that "Compare & Pull request" button appears. Click the button or move into "pull request" tab if you cannot find the button.

When you create a pull request, you choose the branches to compare and merge. Choose the base as `apache:master` and the head `{your_alias}:{branch_name}`. The description will be the message of your one commit. Feel free to edit it if you aren't satisfied with your commit message.

Please **update the JIRA issue with a link to the Pull Request**. This triggers an email to the mailing list, which will hopefully result in a committer stepping up for a code review.

#####7. The code review

REEF follows a review-then-commit (RTC) model. We perform all our code reviews on GitHub: Community members will leave comments on your pull request and suggest changes. You can have [a look at prior pull requests](https://github.com/apache/incubator-reef/pulls?q=is%3Apr+is%3Aclosed) to get an idea of what to expect. During this process, you may want to change your code. You can do that by pushing additional commits to your branch. Don't worry about squashing the commits into one at this stage. This will be done by the committer once your code is merged into REEF.

When the code review concludes, one of Committers will merge your work into the REEF codebase. Good job!

#####8. Merge the pull request (Committers only)

If you are a committer, you can follow the steps in the [Committer Guide](committer-guide.html) to merge the pull request. Of course, you won't be merging your own pull requests. Nudge committers as needed to make sure everything gets merged correctly.

####Other guides

There are other pages in this area you might be interested in:

- [Coding Guidelines](coding-guideline.html)
- [REEF Tutorial](tutorial.html)