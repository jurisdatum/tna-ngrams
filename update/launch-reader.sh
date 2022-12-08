#!/bin/bash
# expects 2 arguments:
#  the subnet id
#  the S3 location of the HBase data
aws emr create-cluster \
  --name "HBase reader" \
  --region eu-west-2 \
  --release-label emr-6.7.0 \
  --applications Name=HBase \
  --use-default-roles \
  --instance-type m4.large \
  --instance-count 1 \
  --ec2-attributes SubnetId=$1 \
  --configurations "[{\"Classification\":\"hbase\",\"Properties\":{\"hbase.emr.storageMode\":\"s3\"}},{\"Classification\":\"hbase-site\",\"Properties\":{\"hbase.rootdir\":\"$2\"}}]" \
  --no-auto-terminate
