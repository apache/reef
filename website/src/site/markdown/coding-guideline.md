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
#Coding Guidelines

###The Basics

We largely follow [Hadoop coding guidelines](http://wiki.apache.org/hadoop/CodeReviewChecklist):

- Use 2 (TWO) spaces to indent code.
- Use LF (Unix style) line endings.
- Do **not** use the `@author` Javadoc tag. (Be modest ! :-))
- For the rest, follow the original [Sun Java coding conventions](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html).
- If some file has different formatting, try to match the existing style.
- Use [CheckStyle](http://checkstyle.sourceforge.net/) to verify your coding style.
- Avoid mixing both style and functionality changes in one commit.
- Make sure all files have the needed [license header](http://www.apache.org/legal/src-headers.html).

###Comments

We require committers to add comments to generate proper javadoc pages. In addition to this requirement, we encourage committers to add comments (if needed) to make code easier to understand. 

- Class
- Interface
- Public constructor
- Public method
- Important fields
- Code part that captures complex logic

###On immutability of why you should make everything final

REEF favors immutable objects over mutable ones. When you browse the code base, you see that there is an enormous number of final and readonly modifiers used. The reason for this stems from the distributed and parallel nature of REEF: What cannot be changed doesn't have to be guarded with locks. Hence, immutability is an excellent tool to achieve both thread safety and performance. Following this line of thought, we arrive at the following guidelines:

- Make all instance variables possible `final`.
- Use the [Java Concurrency in Practice annotations](http://jcip.net.s3-website-us-east-1.amazonaws.com/) for the instance variables that aren't `final`.
- When a class has more than 3 non-`final` instance variables, consider breaking the class in two to limit the lock complexity.
