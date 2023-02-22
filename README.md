# N-grams

This Maven project contains three modules:
* update (code for generating the n-grams data and writing it to HBase)
* api (code for reading the data from HBase and reformatting it in response to HTTP requests)
* common (code shared by the two above, mostly HBase interfaces)

A single `mvn clean package` command will produce the following artifacts:
* ngrams-update-2.0.2.jar (in update/target)
* ngrams-api-2.0.2.war (in api/target)

The first is a Hadoop MapReduce application, designed to be run on AWS EMR. The second is a web application archive that can be served with, e.g., Tomcat.

Generating the data and serving it requires three steps:
1. run the MapReduce application to generate the data
2. launch an HBaser reader instance
3. deploy the API

Inlucded (in the update folder) are a few example Bash scripts to illustrate these steps. They should work, if all the necessary permissions are in place. But they may be more useful just as documentation of the necessary configurations.

## 1. Generate the data

To launch an auto-terminating Hadoop cluster that will generate the n-grams data and store it to S3:
1. create an AWS subnet
2. copy the `ngrams-update-2.0.2.jar` file to S3 (the EMR job below must have permission to read it)
3. create an S3 bucket to store the HBase data (the EMR job below must have permission to write to it)
4. run `launch-update.sh` with the following arguments:
    * the ID of an AWS subnet (e.g., `subnet-0123456789abcdef0`)
    * the location in S3 of the .jar file (e.g., `s3://{bucket}/{path}/ngrams-update-2.0.2.jar`)
    * the location in S3 where the Hbase data should be stored (e.g., `s3://{bucket}/hbase/1`)

For example:
```
./launch-update.sh subnet-0123456789abcdef0 s3://bucket/path/ngrams-update-2.0.2.jar s3://bucket/hbase/1
```

The MapReduce application will execute the following steps:

| Action | Duration |
| ------ | -------- |
| step 1: enumerate years for doc types | 23 s |
| step 2: scrape LGU for doc ids | 2.3 h |
| step 3: write documents to HBase | 36 h |
| step 4: write counts to HBase | 10.3 h |
| step 5.1: write counts for primary/secondary groups | 4.9 h |
| Step 5.2: write counts for uk/eu groups | 9.9 h |
| Step 5.3: write counts for 'all' group | 9.8 h |
 Step 6: generate scaling factors | 7.5 h |
| Step 7: write search data to HBase | 30 h |

## 2. Launch HBase reader

To launch a _non-terminating,_ single-node Hadoop cluster to read the HBase data, run `launch-reader.sh` with the following arguments:
* the ID of the AWS subnet
* the location in S3 where the HBase data was stored

For example:
```
./launch-reader.sh subnet-0123456789abcdef0 s3://bucket/hbase/1
```
## 3. Deploy the API

Install `ngrams-api-2.0.2.war` on any Tomcat server.

Make sure the server has permissions to conect to the HBase reader, e.g, by allowing inbound access to the HBase reader from the API's security group.

Set environment variable `hbase.zookeeper.quorum` to the host of the HBase reader (e.g., `ec2-0-0-0-0.eu-west-2.compute.amazonaws.com`). One way to do this is to add the following line to Tomcat's `setenv.sh` file:
```
export JAVA_OPTS="-Dhbase.zookeeper.quorum=ec2-0-0-0-0.eu-west-2.compute.amazonaws.com"
```
