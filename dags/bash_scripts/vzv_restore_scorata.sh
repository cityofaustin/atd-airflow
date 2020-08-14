#!/usr/bin/env bash

set -o errexit;

# It should attempt to download the latest files
function download_vzv_backup {
  TYPE=$1;
  echo "Downloading From S3: ${TYPE}";
  aws s3 cp "s3://${VZV_DATA_BUCKET_BACKUPS}/latest-${TYPE}.csv.gz" $TYPE.csv.gz;
  echo "Decompressing: ${TYPE}";
  zcat $TYPE.csv.gz > $TYPE.csv;
  echo "Quick Head Test (10 lines): ${TYPE}";
  head -10 $TYPE.csv;
}

# Attempts to upsert to socrata (not implemented)
function upsert_socrata {
  TYPE=$1;
  echo "Upserting to Socrata: ${TYPE}";
  python3 ~/dags/bash_scripts/vzv_restore_socrata.py $TYPE;
}

for BACKUP_TYPE in crashes persons;
do
  download_vzv_backup $BACKUP_TYPE;
  upsert_socrata $BACKUP_TYPE;
done;
