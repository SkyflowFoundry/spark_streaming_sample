Requriments:
Maven and Java
Make sure you have a vault (schema is in vaultSchema.json). Add its details to config.yml
Make sure you have a service accounts. Download credentials.json. Update config if needed.

Build:
data-generator (and loader)
Go to data-gen directory
mvn clean (if needed)
mvn package assembly:single
Now target has a jar file named: SyntheticDataGenerator-1.0-SNAPSHOT-jar-with-dependencies.jar

Running:
To generate data update config.yml and run:
java -cp target/SyntheticDataGenerator-1.0-SNAPSHOT-jar-with-dependencies.jar com.skyflow.walmartpoc.GenSeedData path/to/config.yml path/to/desired/outputdir

Results in:
Synthetic customers and payments data generated.
CSV files being created as specified in the config.yml.
Data loaded into the vault.