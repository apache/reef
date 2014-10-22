REEF
===== 

REEF is the Retainable Evaluator Execution Framework. It makes it easier to write applications on top of resource managers (e.g. YARN). It is released under the Apache 2.0 license. To get started, there is a [Tutorial](https://github.com/Microsoft-CISL/REEF/wiki/How-to-download-and-compile-REEF). We also have a [website](http://www.reef-project.org/)


THIRD PARTY SOFTWARE.
---------------------
This software is built using Maven.  Maven allows you to obtain software libraries from other sources as part of the build process.  Those libraries are offered and distributed by third parties under their own license terms.  Microsoft is not developing, distributing or licensing those libraries to you, but instead, as a convenience, enables you touse this software to obtain those libraries directly from the creators or providers.  By using the software, you acknowledge and agree that you are nbtaining the libraries directly from the third parties and under separate license terms, and that it is your responsibility to locate, understand and comply with any such license terms.  Microsoft grants you no license rights for third-party software or libraries that are obtained using this software.

The list of libraries pulled in this way includes, but is not limited to:

 * aopalliance:aopalliance:jar:1.0:compile
 * asm:asm:jar:3.3.1:compile
 * cglib:cglib:jar:2.2.2:compile
 * com.google.guava:guava:jar:15.0:compile
 * com.google.inject:guice:jar:3.0:compile
 * com.google.inject.extensions:guice-servlet:jar:3.0:compile
 * com.google.protobuf:protobuf-java:jar:2.5.0:compile
 * com.jcraft:jsch:jar:0.1.42:compile
 * com.sun.jersey:jersey-client:jar:1.9:compile
 * com.sun.jersey:jersey-core:jar:1.9:compile
 * com.sun.jersey:jersey-grizzly2:jar:1.9:compile
 * com.sun.jersey:jersey-json:jar:1.9:compile
 * com.sun.jersey:jersey-server:jar:1.9:compile
 * com.sun.jersey.contribs:jersey-guice:jar:1.9:compile
 * com.sun.jersey.jersey-test-framework:jersey-test-framework-core:jar:1.9:compile
 * com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:compile
 * com.sun.xml.bind:jaxb-impl:jar:2.2.3-1:compile
 * com.thoughtworks.paranamer:paranamer:jar:2.3:compile
 * commons-cli:commons-cli:jar:1.2:compile
 * commons-codec:commons-codec:jar:1.4:compile
 * commons-configuration:commons-configuration:jar:1.9:compile
 * commons-el:commons-el:jar:1.0:runtime
 * commons-httpclient:commons-httpclient:jar:3.1:compile
 * commons-io:commons-io:jar:2.1:compile
 * commons-lang:commons-lang:jar:2.6:compile
 * commons-logging:commons-logging:jar:1.1.1:compile
 * commons-net:commons-net:jar:3.1:compile
 * dom4j:dom4j:jar:1.6.1:compile
 * io.netty:netty:jar:3.5.10.Final:compile
 * javax.activation:activation:jar:1.1:compile
 * javax.inject:javax.inject:jar:1:compile
 * javax.servlet:javax.servlet-api:jar:3.0.1:compile
 * javax.servlet:servlet-api:jar:2.5:compile
 * javax.servlet.jsp:jsp-api:jar:2.1:runtime
 * javax.xml.bind:jaxb-api:jar:2.2.2:compile
 * jdk.tools:jdk.tools:jar:1.7:system
 * junit:junit:jar:4.11:test
 * log4j:log4j:jar:1.2.17:compile
 * net.java.dev.jets3t:jets3t:jar:0.6.1:compile
 * org.apache.avro:avro:jar:1.7.4:compile
 * org.apache.commons:commons-compress:jar:1.4.1:compile
 * org.apache.commons:commons-math:jar:2.1:compile
 * org.apache.hadoop:hadoop-annotations:jar:2.2.0:compile
 * org.apache.hadoop:hadoop-auth:jar:2.2.0:compile
 * org.apache.hadoop:hadoop-common:jar:2.2.0:compile
 * org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:compile
 * org.apache.hadoop:hadoop-yarn-client:jar:2.2.0:compile
 * org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:compile
 * org.apache.zookeeper:zookeeper:jar:3.4.5:compile
 * org.codehaus.jackson:jackson-core-asl:jar:1.8.8:compile
 * org.codehaus.jackson:jackson-jaxrs:jar:1.8.3:compile
 * org.codehaus.jackson:jackson-mapper-asl:jar:1.8.8:compile
 * org.codehaus.jackson:jackson-xc:jar:1.8.3:compile
 * org.codehaus.jettison:jettison:jar:1.1:compile
 * org.glassfish:javax.servlet:jar:3.1:compile
 * org.glassfish.external:management-api:jar:3.0.0-b012:compile
 * org.glassfish.gmbal:gmbal-api-only:jar:3.0.0-b023:compile
 * org.glassfish.grizzly:grizzly-framework:jar:2.1.2:compile
 * org.glassfish.grizzly:grizzly-http:jar:2.1.2:compile
 * org.glassfish.grizzly:grizzly-http-server:jar:2.1.2:compile
 * org.glassfish.grizzly:grizzly-http-servlet:jar:2.1.2:compile
 * org.glassfish.grizzly:grizzly-rcm:jar:2.1.2:compile
 * org.hamcrest:hamcrest-core:jar:1.3:test
 * org.javassist:javassist:jar:3.16.1-GA:compile
 * org.mockito:mockito-core:jar:1.9.5:test
 * org.mortbay.jetty:jetty:jar:6.1.26:compile
 * org.mortbay.jetty:jetty-util:jar:6.1.26:compile
 * org.objenesis:objenesis:jar:1.0:test
 * org.reflections:reflections:jar:0.9.9-RC1:compile
 * org.slf4j:slf4j-api:jar:1.7.5:compile
 * org.slf4j:slf4j-log4j12:jar:1.7.5:compile
 * org.tukaani:xz:jar:1.0:compile
 * org.xerial.snappy:snappy-java:jar:1.0.4.1:compile
 * stax:stax-api:jar:1.0.1:compile
 * tomcat:jasper-compiler:jar:5.5.23:runtime
 * tomcat:jasper-runtime:jar:5.5.23:runtime
 * xml-apis:xml-apis:jar:1.0.b2:compile
 * xmlenc:xmlenc:jar:0.52:compile

An up-to-date list of dependencies pulled in this way can be generated via `mvn dependency:list` on the command line.
