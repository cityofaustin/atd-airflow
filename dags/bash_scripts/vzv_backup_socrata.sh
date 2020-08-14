#!/usr/bin/env bash

set -o errexit;

# Attempts to download the full dataset (compressed)
function create_backup_dataset {
  TYPE=$1;
  case "${TYPE}" in
        crashes)
            export DATASET_CODE=$VZV_DATA_CRASHES;
            ;;
        persons)
            export DATASET_CODE=$VZV_DATA_PERSONS;
            ;;
        *)
            echo "Can't create backup, invalid type: ${TYPE}";
            exit 1
  esac;

  echo "Backing up Dataset: ${TYPE} : Dataset Code: '${DATASET_CODE}'";
  curl --silent --location --request GET \
      --header "Cache-Control: no-cache" \
      --header "Accept: */*" \
      --header "Accept-Encoding: gzip, deflate, br" \
      --header "Connection: keep-alive" \
      "https://data.austintexas.gov/resource/${DATASET_CODE}.csv?\$limit=9999999" \
      --output $TYPE.csv.gz;

  echo "Backup sample: ";
  zcat $TYPE.csv.gz | head -10;
}

# Uploads to S3
function upload_to_s3 {
  TYPE=$1;
  echo "Uploading to S3: ${TYPE}";
  aws s3 cp $TYPE.csv.gz "s3://${VZV_DATA_BUCKET_BACKUPS}/socrata-${TYPE}-$(date +"%m-%d-%y").csv.gz";
  echo "Uploading to S3: ${TYPE} (latest)";
  aws s3 cp $TYPE.csv.gz "s3://${VZV_DATA_BUCKET_BACKUPS}/latest-${TYPE}.csv.gz";
}

# Removes the csv.gz file
function cleanup {
  TYPE=$1;
  echo "Removing temporary files: ${TYPE}";
  rm -rf $TYPE.csv.gz;
}


for TYPE in crashes persons;
do
  create_backup_dataset $TYPE;
  upload_to_s3 $TYPE;
  cleanup $TYPE;
done;