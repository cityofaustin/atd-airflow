#!/usr/bin/env python3

import os, sys, time
from sodapy import Socrata

# First we need to get the first argument into the MODE variable: (crashes, persons)
MODE = sys.argv[1]
# We will attempt to get the environment variable containing the dataset unique id
SOCRATA_DATASET_UID = os.getenv(f"SOCRATA_DATASET_{str(MODE).upper()}", "")

if MODE == "":
    print("No mode specified, quitting restore.")
    exit(1)
else:
    MODE = str(MODE).lower()

if SOCRATA_DATASET_UID == "":
    print("Could not fetch dataset UID, quitting restore.")
    exit(1)

# Start timer
start = time.time()
print(f"Socrata - Restore (Mode: {str(MODE).upper()}, uuid: {SOCRATA_DATASET_UID}):  Connecting...")

# Setup connection to Socrata
client = Socrata(
    "data.austintexas.gov",
    os.getenv("SOCRATA_APP_TOKEN", ""),
    username=os.getenv("SOCRATA_KEY_ID", ""),
    password=os.getenv("SOCRATA_KEY_SECRET", ""),
    timeout=20,
)

print("Connected!")

print(f"Truncating crashes table...")
client.replace(SOCRATA_DATASET_UID, [])
print(f"Opening backup file: '{MODE}.csv'")
data = open(f"{MODE}.csv")
print(f"Upserting data, this can take a few minutes")
# client.replace(SOCRATA_DATASET_UID, data)

# Terminate Socrata connection

client.close()
print("Connection Terminated")

# Stop timer and print duration
end = time.time()
hours, rem = divmod(end - start, 3600)
minutes, seconds = divmod(rem, 60)
print("Finished in: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))