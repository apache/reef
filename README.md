Wake
====
Wake is an event-driven framework based on ideas from SEDA, Click, Akka and Rx.

Background - thread pools, context switching overheads, teardown / setup compleixity / performance / debugging

Core API

Helper libraries

Stage implementations

Profiling

Debugging

THIRD PARTY SOFTWARE 
--------------------
This software is built using Maven.  Maven allows you
to obtain software libraries from other sources as part of the build process.
Those libraries are offered and distributed by third parties under their own
license terms.  Microsoft is not developing, distributing or licensing those
libraries to you, but instead, as a convenience, enables you to use this
software to obtain those libraries directly from the creators or providers.  By
using the software, you acknowledge and agree that you are obtaining the
libraries directly from the third parties and under separate license terms, and
that it is your responsibility to locate, understand and comply with any such
license terms.  Microsoft grants you no license rights for third-party software
or libraries that are obtained using this software.

The list of libraries pulled in this way includes, but is not limited to:

 * asm:asm:jar:3.3.1:compile
 * cglib:cglib:jar:2.2.2:compile
 * com.google.code.findbugs:jsr305:jar:1.3.9:compile
 * com.google.guava:guava:jar:11.0.2:compile
 * com.google.protobuf:protobuf-java:jar:2.5.0:compile
 * commons-cli:commons-cli:jar:1.2:compile
 * commons-configuration:commons-configuration:jar:1.9:compile
 * commons-lang:commons-lang:jar:2.6:compile
 * commons-logging:commons-logging:jar:1.1.1:compile
 * dom4j:dom4j:jar:1.6.1:compile
 * io.netty:netty:jar:3.5.10.Final:compile
 * javax.inject:javax.inject:jar:1:compile
 * junit:junit:jar:4.10:test
 * org.hamcrest:hamcrest-core:jar:1.1:test
 * org.javassist:javassist:jar:3.16.1-GA:compile
 * org.reflections:reflections:jar:0.9.9-RC1:compile
 * xml-apis:xml-apis:jar:1.0.b2:compile


An up-to-date list of dependencies pulled in this way can be generated via `mvn dependency:list` on the command line.
