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
#Apache REEF&trade;

###What is REEF?

**REEF**, the Retainable Evaluator Execution Framework, is our approach to simplify and unify the lower layers of big data systems on modern resource managers.

For managers like Apache YARN, Apache Mesos, Google Omega, and Facebook Corona, REEF provides a centralized control plane abstraction that can be used to build a decentralized data plane for supporting big data systems. Special consideration is given to graph computation and machine learning applications, both of which require data *retention* on allocated resources to execute multiple passes over the data.

More broadly, applications that run on YARN will have the need for a variety of data-processing tasks e.g., data shuffle, group communication, aggregation, checkpointing, and many more. Rather than reimplement these for each application, REEF aims to provide them in a library form, so that they can be reused by higher-level applications and tuned for a specific domain problem e.g., Machine Learning.

In that sense, our long-term vision is that REEF will mature into a Big Data Application Server, that will host a variety of tool kits and applications, on modern resource managers.

<div style="text-align:center" markdown="1">
    <img>
        <img src ="REEFDiagram.png"/>
    </img>
</div>

###How can I get started?

The official home for the REEF (and Tang and Wake) source code is at the Apache Software Foundation. You can check out the current code via:

    $ git clone git://git.apache.org/reef.git

or directly access its GitHub page [here](https://github.com/apache/reef).

Detailed information about REEF and using it can be found in the [FAQ](faq.html) and the [Tutorial](https://cwiki.apache.org/confluence/display/REEF/Tutorials).

If you wish to contribute, start at the [Contributing](https://cwiki.apache.org/confluence/display/REEF/Contributing) tutorial or the [Committer Guide](https://cwiki.apache.org/confluence/display/REEF/Committer+Guide)!

###Further questions?

Please visit our [Frequently Asked Questions](faq.html) page or use our [Mailing List](mailing-list.html) to send us a question!
