#!/bin/bash

# Replace with your actual bucket name
BUCKET_NAME=ds-youtube-raw-uswest2-dev

# To copy all JSON reference data into the designated reference path
aws s3 cp . s3://$BUCKET_NAME/youtube/raw_statistics_reference_data/ --recursive --exclude "*" --include "*.json"

# To copy CSVs to region-partitioned locations following Hive-style directory format
aws s3 cp CAvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=CA/
aws s3 cp DEvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=DE/
aws s3 cp FRvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=FR/
aws s3 cp GBvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=GB/
aws s3 cp INvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=IN/
aws s3 cp JPvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=JP/
aws s3 cp KRvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=KR/
aws s3 cp MXvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=MX/
aws s3 cp RUvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=RU/
aws s3 cp USvideos.csv s3://$BUCKET_NAME/youtube/raw_statistics/region=US/
