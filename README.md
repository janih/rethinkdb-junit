#rethinkdb-junit#

This is a JUnit test project to reproduce https://github.com/rethinkdb/rethinkdb/issues/2410

##Running the test##

Requirements:

* [JDK 7/8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Maven 3](http://maven.apache.org)
* [RethinkDB](http://rethinkdb.com/docs/install)

Run:

1. Check and configure *junit.properties*.
2. Have RethinkDB running. The test will try to create a new db and insert data to it.
3. Run:

    mvn install

Maven should download the required dependencies and start running the test.


##Creating Ecplise project files##

    mvn eclipse:eclipse -DdownloadJavadocs=true -DdownloadSources=true
