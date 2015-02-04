Website
===========

##Instructions

####Generating the HTML files

Navigate to the {$REEF_HOME}/website directory then use the command:

	mvn clean site:site

This will generate a directory named "target" and inside, there will be a directory named "site". The site folder contains all files required for the website and index.html is the default page shown when the domain is first accessed.

####Generating the Javadocs files for REEF

Navigate to {$REEF_HOME} then use the command:
	
	mvn javadoc:aggregate

This will create a directory named "apidocs" which includes all the necessary Javadocs for REEF.

####Combining the HTML files with Javadocs files

The site.xml file automatically searches for the apidocs at `apidocs/{$REEF_VERSION}/index.html`. When copying the apidocs to the svn, it is crucial to use this exact path because multiple index.html's exist and they must be referenced without ambiguity.