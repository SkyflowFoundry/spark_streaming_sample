Requriments:
JDK version 1.8. Don't get a later version. Or earlier.
Scala > 2.12. I have Scala 3
Maven
Make sure you have a vault (schema is in vaultSchema.json). Add its details to config.yml
Make sure you have a service accounts. Download credentials.json. Update config as needed.

data-generator
Build:
Go to data-gen directory
mvn clean (if needed)
mvn package install assembly:single
Now target has a jar file named: SyntheticDataGenerator-1.0-SNAPSHOT-jar-with-dependencies.jar

Running:
To generate data update config.yml and run:
java -cp target/SyntheticDataGenerator-1.0-SNAPSHOT-jar-with-dependencies.jar com.skyflow.walmartpoc.<class with main> path/to/config.yml <other-args>


emr-task:
Prereqs: data-generator is built & deployed to local maven repo
Build:
mvn package assembly:single
Now, target has a jar file emr-task-<version>-jar-with-dependencies.jar