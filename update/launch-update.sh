#!/bin/bash
# expects 3 arguments:
#  the subnet id
#  the S3 location of the .jar file
#  the S3 location for HBase data
sed "s|s3:\/\/BUCKET\/PATH|$3|g" configurations.json > temp_config.json
aws emr create-cluster --name "Ngrams update" \
  --region eu-west-2 \
  --release-label emr-6.7.0 \
  --applications Name=Hadoop Name=HBase \
  --use-default-roles \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
    'InstanceGroupType=CORE,InstanceType=m5.2xlarge,InstanceCount=3,EbsConfiguration={EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=gp2,SizeInGB=512}}]}' \
  --ec2-attributes SubnetId=$1 \
  --configurations  file://temp_config.json \
  --steps Type=CUSTOM_JAR,Jar=$2 \
  --auto-terminate
